import sqlite3
import sys

from copy import copy
from enum import Enum
from getopt import getopt
from os import kill
from socket import socket, AF_INET, SOCK_STREAM
from socketserver import TCPServer, ThreadingMixIn, StreamRequestHandler
from ssl import wrap_socket, CERT_NONE, CERT_REQUIRED, PROTOCOL_TLSv1
from struct import pack, unpack
from threading import Condition, Lock, RLock, Thread
from time import sleep
from traceback import format_exc, print_stack
from uuid import uuid4

from subprocess import Popen, STDOUT
try:
    from subprocess import CREATE_NEW_PROCESS_GROUP

    # DETACHED_PROCESS is a creation flag for Popen that can be imported from
    # the win32process module if pywin32 is installed, or manually defined
    DETACHED_PROCESS = 0x00000008

    WIN32_POPEN_FLAGS = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
except:
    # running on non-Windows platform
    WIN32_POPEN_FLAGS = None

DEFAULT_TCP_PORT = 9990
DEFAULT_SSL_PORT = 9991

DEFAULT_SSL_PEM_FILE = 'server.pem'
DEFAULT_SSL_KEY_FILE = 'server.key'

# sets buffer size for network reads
MAX_BUF_LEN = 4096

# prefixes network messages
SUPERQ_MSG_HEADER_BYTE = 42

# superq network node supported commands
SQNodeCmd = Enum('SQNodeCmd', 'superq_exists '
                              'superq_create '
                              'superq_read '
                              'superq_delete '
                              'superq_query '
                              'superqelem_exists '
                              'superqelem_create '
                              'superqelem_read '
                              'superqelem_update '
                              'superqelem_delete')

# local process datastore serving either user program or network node
_dataStore = None
_dataStoreLock = Lock()

def shutdown():
    if _dataStore:
        _dataStore.shutdown()

def log(msg):
    with open('node.output', 'a') as f:
        f.write('\n' + msg)

# base superq exception
class SuperQEx(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class NotImplemented(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class DBExecError(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MalformedNetworkRequest(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MalformedNetworkResponse(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class SuperQEmpty(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class SuperQFull(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ObjectNotRecognized(SuperQEx):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

# simple linked list element. superqelem inherits from this
class LinkedListNode():
    def __init__(self):
        self.prev = None
        self.next = None

# doubly-linked list implementation used by superq and superqelem
class LinkedList():
    def __init__(self, circular = False):
        self.head = None
        self.tail = None
        self.__count = 0

        # llist can iterate circularly
        self.circular = circular

    def __len__(self):
        return self.__count

    def __iter__(self):
        self.__next = self.head
        return self

    def __next__(self):
        returnObj = self.__next

        if returnObj is None:
            if self.circular:
                self.__next = self.head
                returnObj = self.__next
            else:
                raise StopIteration

        self.__next = returnObj.next

        return returnObj

    def __lookup(self, idx):
        if idx >= self.__count:
            raise IndexError('idx ({0}/{1}) out of range'.format(idx,
                                                                 len(self)))

        if self.__count == 0:
            return None

        if idx >= 0:
            # start from whichever end of list is closest to idx
            midIdx = (self.__count - 1) // 2
            if idx < midIdx:
                item = self.head
                for i in range(0, idx):
                    item = item.next
            else:
                item = self.tail
                for i in range(0, (self.__count - 1) - idx):
                    item = item.prev
        else:
            # handle negative idx
            item = self.tail
            for i in range(1, abs(idx)):
                item = item.prev

        return item

    def __slice(self, slice_):
        newLst = LinkedList()

        start = slice_.start
        stop = slice_.stop
        step = slice_.step

        if step == None:
            step = 1

        if start == None:
            if step < 0:
                start = -1
            else:
                start = 0
        elif start < 0:
            start = len(self) + start

        if abs(start) > len(self):
            raise IndexError('list index out of range')

        if stop == None:
            stop = len(self)
        elif stop < 0:
            stop = len(self) - stop

        if abs(stop) > len(self):
            raise IndexError('list index out of range')

        node = self.__getitem__(start)
        for i in range(*slice_.indices(len(self))):
            newLst.push_tail(copy(node))
            steps = step
            while steps != 0:
                if steps < 0:
                    node = node.prev
                    if node is None:
                        node = self.tail
                    steps += 1
                else:
                    node = node.next
                    if node is None:
                        node = self.head
                    steps -= 1

        return newLst

    def __getitem__(self, val):
        if isinstance(val, slice):
            return self.__slice(val)

        return self.__lookup(val)

    def is_empty(self):
        return self.__count == 0

    def push(self, idx, node):
        if idx == 0:
            # set new head, order of operations matters
            node.prev = None
            node.next = self.head

            # if list not empty, point current head to new head
            if self.head is not None:
                self.head.prev = node
            else:
                self.tail = node

            self.head = node
        elif idx >= self.__count:
            # set new tail, order of operations matters
            node.next = None
            node.prev = self.tail

            # if list not empty, point current tail to new tail
            if self.tail is not None:
                self.tail.next = node
            else:
                self.head = node

            self.tail = node
        else:
            curNode = self.__lookup(idx)

            # handle empty list case
            if curNode is None:
                self.head = node
                self.tail = node
                node.prev = None
                node.next = None
            else:
                # splice new node in
                node.next = curNode
                node.prev = curNode.prev
                curNode.prev.next = node
                curNode.prev = node

        self.__count += 1

    def push_head(self, node):
        self.push(0, node)

    def push_middle(self, idx, node):
        self.push(idx, node)

    def push_tail(self, node):
        self.push(self.__count, node)

    def pop(self, idx):
        if self.__count < 1:
            return None
        
        if idx <= 0:
            # get list head
            item = self.head

            # point list head to next element
            self.head = self.head.next

            # if list not empty, tell head it has no prev         
            if self.head is not None:
                self.head.prev = None

            # one less element in the list
            self.__count -= 1

            # if down to 1 element, set tail to head
            if self.__count == 1:
                self.tail = self.head
                
            return item
        elif idx >= self.__count - 1:
            # get list tail
            item = self.tail

            # point list tail to previous element
            self.tail = self.tail.prev

            # if list not empty, tell tail it has no next
            if self.tail is not None:
                self.tail.next = None

            # one less element in the list
            self.__count -= 1

            # if down to 1 element, set head to tail
            if self.__count == 1:
                self.head = self.tail

            return item
        else:
            curNode = self.__lookup(idx)

            # because curNode is not head or tail, these dereferences are safe
            curNode.prev.next = curNode.next
            curNode.next.prev = curNode.prev

            # one less element in the list
            self.__count -= 1

            return curNode
          
    def pop_head(self):
        return self.pop(0)

    def pop_middle(self, idx):
        return self.pop(idx)

    def pop_tail(self):
        return self.pop(self.__count - 1)

    def pop_node(self, node):
        if node is None:
            return None

        if node.prev is not None:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next is not None:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

        self.__count -= 1

        return node

    def insert_before(self, oldNode, newNode):
        if self.__count < 1:
            raise SuperQEx('Calling insert_before() on empty list.')

        # handle case inserting new head
        if self.head == oldNode:
            newNode.prev = None
            newNode.next = oldNode
            oldNode.prev = newNode
            self.head = newNode
        else:
            newNode.prev = oldNode.prev
            newNode.next = oldNode
            oldNode.prev.next = newNode
            oldNode.prev = newNode

        self.__count += 1

    def insert_after(self, oldNode, newNode):
        if self.__count < 1:
            raise SuperQEx('Calling insert_before() on empty list.')

        # handle case inserting new tail
        if self.tail == oldNode:
            newNode.next = None
            newNode.prev = oldNode
            oldNode.next = newNode
            self.tail = newNode
        else:
            newNode.prev = oldNode
            newNode.next = oldNode.next
            oldNode.next.prev = newNode
            oldNode.next = newNode

        self.__count += 1

    def move_up(self, node):
        # can't move list node up if it is already head
        if node.prev is None:
            return

        # these are aliases to the 4 starting elements involved
        above_node_prev = node.prev.prev
        above_node = node.prev
        current_node = node
        current_node_next = node.next

        # do the pointer swaps
        if above_node_prev is not None:
            above_node_prev.next = current_node
        current_node.prev = above_node_prev
        current_node.next = above_node
        above_node.prev = current_node
        above_node.next = current_node_next
        above_node.next.prev = above_node

        # if node is at top of list, set head to node
        if current_node.prev is None:
            self.head = current_node

    def move_down(self, node):
        # can't move list node up if it is already head
        if node.prev is None:
            return

        # these are aliases to the 4 starting elements involved
        above_node_prev = node.prev.prev
        above_node = node.prev
        current_node = node
        current_node_next = node.next

        # do the pointer swaps
        if above_node_prev is not None:
            above_node_prev.next = current_node
        current_node.prev = above_node_prev
        current_node.next = above_node
        above_node.prev = current_node
        above_node.next = current_node_next
        above_node.next.prev = above_node

        # if node is at top of list, set head to node
        if current_node.prev is None:
            self.head = current_node

def db_exec(dbConn, sql):
    while True:
        try:
            dbConn.execute(sql)
            break
        except sqlite3.OperationalError:
            # when using shared cache mode, sqlite ignores timeouts and
            # handlers, requiring for now this spinning solution.
            # shared cache mode is needed for parallel access of memory db
            sleep(.01)
        except Exception as e:
            raise DBExecError('query: {0}\nException: {1}'.format(sql,
                                                                  str(e)))

    dbConn.commit()

def db_select(dbConn, sql):
    rowLst = []
    dbConn.row_factory = sqlite3.Row
    try:
        result = dbConn.execute(sql)
    except Exception as e:
        raise DBExecError('query: {0}\nException: {1}'.format(sql,
                                                              str(e)))

    for row in result:
        rowLst.append(row)

    return rowLst

def db_create_table(dbConn, tableName, colStr):
    db_exec(dbConn, 'CREATE TABLE {0} ({1});'.format(tableName,
                                                     colStr))

def db_delete_table(dbConn, tableName):
    db_exec(dbConn, 'DROP TABLE {0};'.format(tableName))

def db_create_row(dbConn, tableName, colStr, valStr):
    db_exec(dbConn, 'INSERT INTO {0} ({1}) VALUES ({2});'.format(tableName,
                                                                 colStr,
                                                                 valStr))

def db_update_row(dbConn, tableName, updateStr, keyName, keyVal):
    db_exec(dbConn, 'UPDATE {0} SET {1} WHERE {2} = {3};'.format(tableName,
                                                                 updateStr,
                                                                 keyName,
                                                                 keyVal))
            
def db_delete_row(dbConn, tableName, keyName, keyVal):
    db_exec(dbConn, 'DELETE FROM {0} WHERE {1} = {2};'.format(tableName,
                                                              keyName,
                                                              keyVal))

# instantiated for each superq app and for each network node process
class SuperQDataStore():
    def __init__(self):
        # networked datastores will set this to True
        self.public = False

        self._dataStoreBigLock = Lock()

        # keyed by sq.publicName, stores local sqs, caches remote ones
        self.superqdict = {}

        self.__networkClient = None

        # create detached superq to use for sqlite connection pool
        self.__dbConnPool = superq([])

        self.internalConn = self.__new_dbConn()

    def __new_dbConn(self):
        return sqlite3.connect('file:memdb1?mode=memory&cache=shared',
                               uri = True,
                               check_same_thread = False)

    def __get_dbConn(self):
        try:
            dbConn = self.__dbConnPool.pop(block = False)
        except SuperQEmpty:
            return self.__new_dbConn()
        else:
            return dbConn

    def __return_dbConn(self, s):
        self.__dbConnPool.push(s)

    def load_from_file(self):
        raise NotImplemented('SuperQDataStore.load_from_file()')
    
    def save_to_file(self):
        raise NotImplemented('SuperQDataStore.save_to_file()')
    
    def switch_to_disk_based(self):
        raise NotImplemented('SuperQDataStore.switch_to_disk_based()')

    def switch_to_in_memory(self):
        raise NotImplemented('SuperQDataStore.switch_to_in_memory()')

    def __get_networkClient(self):
        if self.__networkClient is None:
            self.__networkClient = SuperQNetworkClientMgr()

        return self.__networkClient

    # potentially starts the network datastore process when accessed
    networkClient = property(__get_networkClient)

    def shutdown(self):
        if self.__networkClient is not None:
            self.__networkClient.shutdown()

    def set_public(self):
        self.public = True

    def superq_exists(self, name, host = None):
        # private datastore call public
        if host is not None and not self.public:
            publicName = '{0}.{1}'.format(host, name)
            return self.networkClient.superq_exists(publicName, host)
        return name in self.superqdict

    def superq_create(self, sq):
        # private datastore call public
        if sq.host is not None and not self.public:
            self.networkClient.superq_create(sq)

        # add superq to dict after locking the entire collection
        self._dataStoreBigLock.acquire()
        if sq.publicName in self.superqdict:
            raise KeyError('superq {0} exists'.format(sq.publicName))
        else:
            self.superqdict[sq.publicName] = sq
        self._dataStoreBigLock.release()
            
    def superq_read(self, name, host = None):
        # private datastore call public
        if host is not None and not self.public:
            publicName = '{0}.{1}'.format(host, name)
            sq = self.networkClient.superq_read(publicName, host)

            if publicName not in self.superqdict:
                sq.attached = True
                self.superqdict[publicName] = sq
            else:
                # rebuild existing superq instance using incoming superq
                knownSq = self.superqdict[publicName]
                knownSq.buildFromStr(str(sq))

                sq = knownSq
        else:
            # expected to raise KeyError if superq not known
            sq = self.superqdict[name]

        return sq

    def superq_delete(self, sq):
        # delete superq from dict after locking the entire collection
        self._dataStoreBigLock.acquire()
        if sq.publicName in self.superqdict:
            del(self.superqdict[sq.publicName])
        else:
            raise KeyError('superq {0} does not exist'.format(sq.publicName))
        self._dataStoreBigLock.release()
        
        # private datastore call public
        if sq.host is not None and not self.public:
            self.networkClient.superq_delete(sq)
            return

        # delete backing table if there are superqelems
        if len(sq) > 0:
            dbConn = self.__get_dbConn()
            db_delete_table(dbConn, sq.name)
            self.__return_dbConn(dbConn)

    def superq_query_local(self, queryStr, objSample = None):
        dbConn = self.__get_dbConn()
        rows = db_select(dbConn, queryStr)
        self.__return_dbConn(dbConn)

        newSq = superq([])

        for row in rows:
            # demarshal single-value objects
            if isinstance(objSample, str):
                newSq.create_elem(str(row['_val_']))
                continue
            elif isinstance(objSample, int):
                newSq.create_elem(int(row['_val_']))
                continue
            elif isinstance(objSample, float):
                newSq.create_elem(float(row['_val_']))
                continue

            if objSample is None:
                newObj = superqelem(parentSq = newSq)
            else:
                newObj = copy(objSample)

            # demarshal multi-value objects
            for col in row.keys():
                # extract field name from col name
                colElems = col.split('.')
                fieldName = colElems[len(colElems) - 1]

                if isinstance(newObj, superqelem):
                    newObj.add_atom(fieldName, 'str', row[fieldName])
                    continue

                objVal = getattr(newObj, fieldName)
                if isinstance(objVal, str):
                    val = str(row[fieldName])
                elif isinstance(objVal, int):
                    val = int(row[fieldName])
                elif isinstance(objVal, float):
                    val = float(row[fieldName])
                else:
                    valType = type(objVal)
                    raise TypeError('unsupported type ({0})'.format(valType))

                setattr(newObj, fieldName, val)

            newSq.create_elem(newObj)

        # clear objSample from being set by first create_elem
        newSq.objSample = None

        return newSq
    
    def superq_query(self, sq, columns, tables, conditional, objSample = None):
        # create column string and list from input
        if isinstance(columns, list):
            colStr = ','.join(columns)
            colLst = columns
        elif isinstance(columns, str):
            colStr = columns
            colLst = columns.split(',')
        else:
            raise TypeError('invalid type ({0})'.format(type(columns)))

        # create table string and list from input
        if isinstance(tables, list):
            tableStr = ','.join(tables)
            tableLst = tables
        elif isinstance(tables, str):
            tableStr = tables
            tableLst = tables.split(',')
        else:
            raise TypeError('invalid type ({0})'.format(type(tables)))

        if '<self>' not in tableStr:
            raise ValueError('join tables ({0}) not valid.'.format(tableStr))

        # do some pre-processing and construct query
        colStr = colStr.replace('<self>', sq.name)
        tableStr = tableStr.replace('<self>', sq.name)
        conditional = conditional.replace('<self>', sq.name)
        queryStr = 'SELECT {0} FROM {1} WHERE {2};'.format(colStr,
                                                           tableStr,
                                                           conditional)

        # execute query locally if superq is not public or the datastore is
        if sq.host is None or self.public:
            return self.superq_query_local(queryStr, objSample)

        resultSq = self.networkClient.superq_query(sq, queryStr)

        if objSample is None:
            return resultSq

        newSq = superq([])

        # if there is a sample object available, demarshal accordingly
        for sqe in resultSq:
            # demarshal single-value objects
            if isinstance(objSample, str):
                newSq.create_elem(str(sqe['_val_']))
                continue
            elif isinstance(objSample, int):
                newSq.create_elem(int(sqe['_val_']))
                continue
            elif isinstance(objSample, float):
                newSq.create_elem(float(sqe['_val_']))
                continue

            newObj = copy(objSample)

            # demarshal multi-value objects
            for atom in sqe:
                col = atom.name

                # extract field name from col name
                colElems = col.split('.')
                fieldName = colElems[len(colElems) - 1]

                objVal = getattr(newObj, fieldName)
                if isinstance(objVal, str):
                    val = str(atom.value)
                elif isinstance(objVal, int):
                    val = int(atom.value)
                elif isinstance(objVal, float):
                    val = float(atom.value)
                else:
                    valType = type(objVal)
                    raise TypeError('unsupported type ({0})'.format(valType))

                setattr(newObj, fieldName, val)

            newSq.create_elem(newObj)

        return newSq

    def superqelem_exists(self, sq, sqeName):
        return sqeName in self.superqdict[sq.publicName][sqeName]

    def superqelem_create(self, sq, sqe, idx = None, createTable = False):
        # private datastore call public
        if sq.host is not None and not self.public:
            self.networkClient.superqelem_create(sq, sqe, idx)
            return

        # the backing db table is only created when the 1st element is added
        if createTable:
            dbConn = self.__get_dbConn()
            db_create_table(dbConn, sq.name, sq.nameTypeStr)
            self.__return_dbConn(dbConn)

        valStr = ''
        if sqe.value is not None:
            name = sqe.name
            if isinstance(name, str):
                name = "'{0}'".format(name)

            value = sqe.value
            if isinstance(value, str):
                value = "'{0}'".format(value)

            valStr += '{0},{1}'.format(name, value)
        else:
            atomDict = sqe.dict()
            for colName in sq.colNames:
                # support autoKey
                if colName == '_name_':
                    valStr += "'{0}',".format(sqe.name)
                    continue
                elif colName == '_links_':
                    continue;

                atom = atomDict[colName]

                if atom.type.startswith('str'):
                    valStr += "'{0}',".format(atom.value)
                else:
                    valStr += str(atom.value) + ','
            valStr = valStr.rstrip(',')

        valStr += ",'{0}'".format(sqe.links)

        dbConn = self.__get_dbConn()
        db_create_row(dbConn, sq.name, sq.nameStr, valStr)
        self.__return_dbConn(dbConn)

    def __superqelem_update_db(self, sq, sqe):
        # support autoKey
        keyCol = sq.keyCol
        if keyCol is None:
            keyCol = '_name_'

        # handle scalars
        if sqe.value is not None:
            val = sqe.value
            if sqe.valueType.startswith('str'):
                val = "'{0}'".format(val)
            
            updateStr = '{0}={1}'.format('_val_', val)
        else:
            updateStr = ''

            for i in range(0, len(sq.colNames)):
                name = sq.colNames[i]

                # no need to update key column
                if name == keyCol or name == '_name_':
                    continue
                elif name == '_links_':
                    # deal with links column after all the rest
                    continue

                val = sqe[sq.colNames[i]]
                if sq.colTypes[i].startswith('str'):
                    val = "'{0}'".format(val)

                updateStr += '{0}={1},'.format(name, val)
            updateStr = updateStr.rstrip(',')

        # special-case _links_ column
        updateStr += ",{0}='{1}'".format('_links_', sqe.links)

        # quote sqe name if it's a string
        sqeName = sqe.name
        if isinstance(sqeName, str):
            sqeName = "'{0}'".format(sqeName)

        dbConn = self.__get_dbConn()
        db_update_row(dbConn, sq.name, updateStr, keyCol, sqeName)
        self.__return_dbConn(dbConn)

    def superqelem_read(self, sq, sqeName):
        # private datastore call public
        if sq.host is not None and not self.public:
            sqe = self.networkClient.superqelem_read(sq.name, sqeName)

            if superqelemExists(sq, sqeName):
                self.__superqelem_update_db(sq, sqe)
            else:
                raise ObjectNotRecognized('sqe \'' + sqeName + '\' not known.')

            self.superqdict[sq.publicName][sqeName] = sqe
            return sqe

        # raises KeyValue error if superqelem not known
        return self.superqdict[sq.publicName][sqeName]

    def superqelem_update(self, sq, sqe):
        # private datastore call public
        if sq.host is not None and not self.public:
            sqe = self.networkClient.superqelem_update(sq, sqe)
            return

        self.__superqelem_update_db(sq, sqe)

    def superqelem_delete(self, sq, sqeName):
        # private datastore call public 
        if sq.host is not None and not self.public:
            self.networkClient.superqelem_delete(sq, sqeName)
            return

        # wrap with quotes if sqe key is str
        if isinstance(sqeName, str):
            sqeName = "'{0}'".format(sqeName)

        # support autoKey
        keyCol = sq.keyCol
        if keyCol is None:
            keyCol = '_name_'

        dbConn = self.__get_dbConn()
        db_delete_row(dbConn, sq.name, keyCol, sqeName)
        self.__return_dbConn(dbConn)

class elematom(LinkedListNode):
    def __init__(self, name, type_, value):
        LinkedListNode.__init__(self)

        self.name = name
        self.type = type_
        self.value = value

class superqelem(LinkedListNode):
    def __init__(self,
                 name = None,
                 value = None,
                 parentSq = None,
                 buildFromStr = False):
        LinkedListNode.__init__(self)

        # list of elematoms
        self.__internalList = LinkedList()

        # dictionary of elematoms, keyed by 'field' name
        self.__internalDict = {}

        # any sqe can link to any number of other sqes
        self.links = ''
        self.linksDict = {}

        if name is None:
            name = str(uuid4().hex)

        self.name = name
        self.value = value

        if self.value is None:
            self.value = name

        self.parentSq = parentSq

        # used to remember user object for local instance
        self.obj = None

        # construct publicName property and add it to the class
        getter = lambda self: self.__get_publicName()
        setattr(self.__class__,
                'publicName',
                property(fget = getter, fset = None))

        if buildFromStr:
            self.__buildFromStr(self.value)
            return

        if not isinstance(self.name, (str, int, float)):
            raise TypeError('invalid name type ({0})'.format(type(self.name)))

        # handle scalars
        self.valueType = ''
        if isinstance(value, (str, int, float)):
            self.valueType = type(self.value).__name__
            return

        # only scalars keep value set
        self.value = None

        # non-scalars should 'remember' the user object they're created from
        self.obj = value

        # handle non-scalars
        self.obj = value
        for attrName in dir(value):
            attr = getattr(value, attrName)

            # skip private attributes and callables
            if attrName.startswith('_') or callable(attr):
                continue

            # ignore any attributes whose types aren't supported
            if not isinstance(attr, (str, int, float)):
                continue

            # add object field as superqelem property
            self.add_property(attrName)

            self.add_atom(attrName, type(attr).__name__, attr)

    def __get_publicName(self):
        if self.parentSq is None:
            return self.name
        return '{0}.{1}'.format(self.parentSq.publicName, self.name)

    def set_scalar(self, value):
        # scalar superqelems don't have properties
        if self.value is None:
            return

        if isinstance(self.value, str):
            value = str(value)
        elif isinstance(self.value, int):
            value = int(value)
        elif isinstance(self.value, float):
            value = float(value)

        self.value = value

        # trigger update
        if self.parentSq is not None:
            self.parentSq.update_elem(self)

    def __setattr__(self, attr, value):
        # handle the setting of links to other sqes
        if (isinstance(value, superqelem) and
            attr != 'prev' and attr != 'next'): # clumsy LinkedList avoidance
            # update link if it exists already
            if attr in self.linksDict:
                oldValue = self.linksDict[attr]
                newValue = value.publicName
                self.links = self.links.replace('{0}^{1}'.format(attr,
                                                                 oldValue),
                                                '{0}^{1}'.format(attr,
                                                                 newValue))
            else:
                self.links += '{0}^{1}/'.format(attr, value.publicName)

            # now set the dictionary value
            self.linksDict[attr] = value.publicName

            # trigger update
            if self.parentSq is not None:
                self.parentSq.update_elem_datastore_only(self)

            # construct read-only property attribute and add it to the class
            getter = lambda self: self.__get_property(attr)
            setattr(self.__class__, attr, property(fget = getter))
        else:
            # call default setattr behavior
            object.__setattr__(self, attr, value)

    def add_property(self, attr):
        # scalar superqelems don't have properties
        if self.value is not None:
            raise TypeError('invalid scalar property')

        # create local setter and getter with a particular attribute name
        getter = lambda self: self.__get_property(attr)
        setter = lambda self, value: self.__set_property(attr, value)

        # construct property attribute and add it to the class
        setattr(self.__class__, attr, property(fget = getter, fset = setter))

    # dynamic property getter
    def __get_property(self, attr):
        # scalar superqelems don't have properties
        if self.value is not None:
            raise TypeError('invalid scalar property')

        if attr in self.__internalDict:
            return self.__internalDict[attr].value
        elif attr in self.linksDict:
            # lookup and return linked sqe
            sqName, sqeName = self.linksDict[attr].rsplit('.', 1)
            return superq(sqName)[sqeName]
        else:
            raise SuperQEx('unrecognized attribute: {0}'.format(attr))

    # dynamic property setter
    def __set_property(self, attr, value):
        # scalar superqelems don't have properties
        if self.value is not None:
            raise TypeError('invalid scalar property')

        # remember attribute
        self.__internalDict[attr].value = value

        # maintain state if there is an original user object
        if self.obj is not None:
            setattr(self.obj, attr, value)

        # trigger update
        if self.parentSq is not None:
            self.parentSq.update_elem_datastore_only(self)

    def resetLinks(self):
        for key in self.linksDict:
            delattr(self.__class__, key)

        self.linksDict = {}
        self.links = ''

    def addLinksFromStr(self, linksStr):
        linkElems = linksStr.split('/')
        for link in linkElems:
            if not link:
                break

            key, value = link.split('^')

            self.links += '{0}^{1}/'.format(key, value)
            self.linksDict[key] = value

            # construct read-only property attribute and add it to the class
            getter = lambda self: self.__get_property(key)
            setattr(self.__class__, key, property(fget = getter))

    def __buildFromStr(self, sqeStr):
        headerSeparatorIdx = sqeStr.index(';')

        # separate out sqe header from remainder
        sqeHeader = sqeStr[ : headerSeparatorIdx]
        sqeBody = sqeStr[headerSeparatorIdx + 1 : ]

        # parse out header fields
        headerElems = sqeHeader.split(',')

        # name-type and name-value
        nameType = headerElems[0]
        if nameType.startswith('str'):
            self.name = str(headerElems[1])
        elif nameType.startswith('int'):
            self.name = int(headerElems[1])
        elif nameType.startswith('float'):
            self.name = float(headerElems[1])

        # value-type and actual value
        self.valueType = headerElems[2]
        if self.valueType.startswith('str'):
            self.value = str(headerElems[3])
        elif self.valueType.startswith('int'):
            self.value = int(headerElems[3])
        elif self.valueType.startswith('float'):
            self.value = float(headerElems[3])

        # add links individually
        self.addLinksFromStr(headerElems[4])

        # scalar superqelems
        if self.valueType != '':
            return

        # only scalar superqelems should use value
        self.value = None

        # number of fields or atoms
        numFields = int(headerElems[5])

        # parse out each field
        for i in range(0, numFields):
            # separate field length indicator from remainder
            separatorIdx = sqeBody.index('|')
            fieldLen = int(sqeBody[ : separatorIdx])
            sqeBody = sqeBody[separatorIdx + 1 : ]

            # slice the rest of the field out
            field = sqeBody[ : fieldLen - 1]
            sqeBody = sqeBody[fieldLen : ]

            # slice field name from field
            separatorIdx = field.index('|')
            fieldName = field[ : separatorIdx]
            field = field[separatorIdx + 1 : ]

            # now retrieve type and value
            separatorIdx = field.index('|')
            fieldType = field[ : separatorIdx]

            fieldValue = field[separatorIdx + 1 : ]
            if fieldType.startswith('int'):
                fieldValue = int(fieldValue)
            elif fieldType.startswith('float'):
                fieldValue = float(fieldValue)

            self.add_atom(fieldName, fieldType, fieldValue)

    def __iter__(self):
        self.iterNext = self.__internalList.head

        return self

    def __next__(self):
        returnObj = self.iterNext

        if returnObj:
            self.iterNext = self.iterNext.next
        else:
            raise StopIteration

        return returnObj

    def __getitem__(self, key):
        if key in self.__internalDict:
            atom = self.__internalDict[key]
        elif isinstance(key, int) and key < len(self.__internalDict):
            # if atom isn't keyed on the int, try the int as an index
            atom = self.__internalList[key]
        else:
            raise KeyError(key)

        return atom.value

    def __setitem__(self, key, value):
        if key in self.__internalDict:
            atom = self.__internalDict[key]
        elif isinstance(key, int) and key < len(self.__internalDict):
            # if atom isn't keyed on the int, try the int as an index
            atom = self.__internalList[key]
        else:
            raise KeyError(key)

        atom.value = value

    def __str__(self):
        sqeStr = '{0},{1},{2},{3},{4},{5};'.format(type(self.name).__name__,
                                                   self.name,
                                                   self.valueType,
                                                   self.value,
                                                   self.links,
                                                   len(self.__internalList))
        for atom in self:
            elemStr = '{0}|{1}|{2};'.format(atom.name, atom.type, atom.value)
            sqeStr += '{0}|{1}'.format(len(elemStr), elemStr)
        
        return sqeStr

    def __basecopy(self):
        # initialize new sqe
        sqe = superqelem(self.name, self.value, self.parentSq)

        # remember user obj
        sqe.obj = self.obj

        # add links individually
        sqe.addLinksFromStr(self.links)

        # add atoms
        for atom in self:
            sqe.add_atom(atom.name, atom.type, atom.value)

        return sqe

    def __copy__(self):
        return self.__basecopy()

    def __deepcopy__(self):
        return self.__basecopy()

    # return internal list
    def _list(self):
        return self.__internalList

    # return internal list as python list
    def list(self):
        return [val for val in self]

    def dict(self):
        return self.__internalDict

    def add_atom(self, name, type_, value):
        atom = elematom(name, type_, value)

        self.__internalDict[name] = atom
        self.__internalList.push_tail(atom)

    def __key_user_obj(self, obj):
        # if possible, make user object relatable back to superqelem
        try:
            setattr(obj, '_superqelemKey', self.name)
        except Exception:
            # one reason for arriving here might be obj is a __slots__ object
            pass
        return obj       

    def demarshal(self, objSample = None):
        # return original user object if it is known
        if self.obj is not None:
            return self.__key_user_obj(self.obj)

        # return superqelem if nothing provided to demarshal into
        if objSample is None:
            return self

        # demarshal single-value objects
        if isinstance(objSample, str):
            return str(self['_val_'])
        elif isinstance(objSample, int):
            return int(self['_val_'])
        elif isinstance(objSample, float):
            return float(self['_val_'])

        # demarshal multi-value objects
        newObj = copy(objSample)
        for name, atom in self.__internalDict.items():
            objVal = getattr(newObj, atom.name)
            if isinstance(objVal, str):
                val = str(atom.value)
            elif isinstance(objVal, int):
                val = int(atom.value)
            elif isinstance(objVal, float):
                val = float(atom.value)
            else:
                raise TypeError('unsupported type ({0})'.format(colType))

            setattr(newObj, atom.name, val)

        return self.__key_user_obj(newObj)

class superq():
    # overriding __new__ in order to be able to return existing objects
    def __new__(cls,
                initObj,
                name = None,
                host = None,
                attach = False,
                keyCol = None,
                maxlen = None,
                buildFromStr = False,
                buildFromFile = False):
        # str initObj can contain string and file deserialization info
        if not buildFromStr and not buildFromFile:
            if isinstance(initObj, str):
                # return datastore superq if it exists
                if _dataStore.superq_exists(initObj, host):
                    return _dataStore.superq_read(initObj, host)
                else:
                    raise KeyError('superq {0} does not exist'.format(initObj))

        return object.__new__(cls)

    def __init__(self,
                 initObj,
                 name = None,
                 host = None,
                 attach = False,
                 keyCol = None,
                 maxlen = None,
                 buildFromStr = False,
                 buildFromFile = False):
        # get DataStore handle
        self.dataStore = _dataStore

        # skip initialization if __init__ is being called on an existing object
        if hasattr(self, 'initialized'):
            return

        # mutex must be held whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning. mutex is
        # shared between the conditions, so acquiring and releasing the
        # conditions also acquires and releases mutex
        self.mutex = RLock()

        # notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then
        self.not_empty = Condition(self.mutex)

        # notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then
        self.not_full = Condition(self.mutex)

        self.name = name

        # if no name provided, one will be assigned
        if self.name is None:
            self.name = 'sq' + str(uuid4().hex)

        # indicates whether superq is currently backed in the datastore
        self.attached = False

        # attached superqs with no host use the private local-process datastore
        self.host = host

        # indicates object field to be used as sqe key
        self.keyCol = keyCol

        # if maxlen is None, superq may grow unbounded
        self.maxlen = maxlen

        # object type is established when the 1st element is added or when
        # superq user manually specifies. This is the type superqelems will
        # be demarshalled into when requested. Or, if None, superqelems will
        # be directly returned (or they can be requested through self.n())
        self.objSample = None

        # these describe the superq backing schema and are populated after
        # using introspection on the 1st element added
        self.colNames = []
        self.colTypes = []
        self.nameStr = ''     # comma-delimited list, usable in INSERTs
        self.nameTypeStr = '' # names and types, usable in CREATEs

        # indicates backing db table should be created next attached add elem
        self.createTable = False

        # superqelems are arrayed like a list but mapped like a dictionary
        self.__internalList = LinkedList()
        self.__internalDict = {}

        # automatically generates key column
        self.autoKey = False
        if self.keyCol is None:
            self.autoKey = True

        # construct publicName property and add it to the class
        getter = lambda self: self.__get_publicName()
        setattr(self.__class__,
                'publicName',
                property(fget = getter, fset = None))

        # deserializes from string or file
        if buildFromStr:
            self.buildFromStr(initObj, attach)
            self.initialized = True
            return
        elif buildFromFile:
            self.buildFromFile(initObj, attach)
            self.initialized = True
            return

        if isinstance(initObj, superq):
            # self.__copy__ and __deepcopy__ arrive here
            for elem in initObj:
                self.create_elem(copy(elem), name = elem.name)
        elif isinstance(initObj, list):
#            if self.name == 'sqMulti':
#                import pdb; pdb.set_trace()
            for item in initObj:
                self.create_elem(item)
        elif isinstance(initObj, dict):
            for key, value in initObj.items():
                self.create_elem(value, name = key)
        else:
            raise TypeError('Unsupported type ({0})'.format(type(initObj)))

        # creates new superq in datastore or attaches to existing one
        if attach:
            self.attach()

        # skip __init__ in the future if superq is returned by __new__
        self.initialized = True

    def __get_publicName(self):       
        if self.host is None:
            return self.name
        return '{0}.{1}'.format(self.host, self.name)

    def __len__(self):
        return len(self.__internalList)

    def __contains__(self, key):
        return key in self.__internalDict

    def __iter__(self):
        self.next = self.__internalList.head

        return self

    def __next__(self):
        elem = self.next

        if self.next:
            self.next = self.next.next
        else:
            raise StopIteration

        # if superqelem is scalar, just return value
        if elem.value is not None:
            return elem.value

        return elem.demarshal(self.objSample)

    def __getitem__(self, val):
        if isinstance(val, slice):
            start, stop, step = val.indices(len(self))

            sq = superq([])
            if start in self.__internalDict:
                sqe = self.__internalDict[start]
                sq.create_elem(copy(sqe))
                while sqe.name != stop:
                    sqe = sqe.next
                    if sqe.name == start:
                        break
                    sq.create_elem(copy(sqe))
            elif isinstance(start, int):
                sqSlice = self.__internalList[val]
                for sqe in sqSlice:
                    sq.create_elem(copy(sqe))
            else:
                raise TypeError('Invalid type ({0})'.format(type(val)))

            return sq
        elif val in self.__internalDict:
            elem = self.__internalDict[val]
        elif isinstance(val, int):
            if val < len(self.__internalDict):
                # if element isn't keyed on the int, use it as an index
                elem = self.__internalList[val]
            else:
                raise KeyError('Invalid key ({0})'.format(val))
        else:
            raise TypeError('Invalid type ({0})'.format(type(val)))

        return self.__unwrap_elem(elem)

    def __setitem__(self, key, value):
        if key in self.__internalDict:
            elem = self.__internalDict[key]
        elif isinstance(key, int) and key < len(self):
            # if element isn't keyed on the int, try the int as an index
            elem = self.__internalList[key]                       
        else:
            self.create_elem(value, key)
            return

        # set scalar value
        if elem.value is not None:
            elem.set_scalar(value)
            return
            
        raise NotImplemented('__setitem__ by index for non-scalars')

    def __delitem__(self, key):
        raise NotImplemented('superq.__delitem__()')

    def __missing__(self, key):
        raise KeyError(key)

    def __basecopy(self):
        return superq(self, name = self.name, attach = False)

    def __copy__(self):
        return self.__basecopy()

    def __deepcopy__(self):
        return self.__basecopy()

    def __str__(self):
        sqHdr = '{0},{1};'.format(self.name, len(self.__internalList))

        # serialize necessary attributes as name-value pairs
        sqAttrs = ''
        sqAttrs += 'host|{0},'.format(self.host)
        sqAttrs += 'keyCol|{0},'.format(self.keyCol)
        sqAttrs += 'maxlen|{0},'.format(self.maxlen)
        sqAttrs += 'autoKey|{0}'.format(self.autoKey)
        sqAttrs += ';'

        sqElems = ''
        for sqe in self.__internalList:
            sqeStr = str(sqe)
            sqElems += '{0},{1}'.format(len(sqeStr), sqeStr)

        sqStr = '{0}{1}{2}'.format(sqHdr, sqAttrs, sqElems)

        return sqStr

    # returns value wrapped in superqelem if it was not already
    def __wrap_elem(self, value, name = None):
        if isinstance(value, superqelem):
            sqe = value
        else:
            # if autoKey on, name will be assigned
            if self.autoKey:
                name = 'sqe' + str(uuid4().hex)
            elif self.keyCol is not None:
                try:
                    name = getattr(value, self.keyCol)
                except:
                    raise KeyError('Key field {0} not found.'.format(name))
            elif name is None:
                raise ValueError('name is None with no autoKey or keyCol')

            sqe = superqelem(name, value, parentSq = self)

        return sqe

    # if elem is scalar, returns value or demarshals sqe if possible
    def __unwrap_elem(self, sqe):
        # if superqelem is scalar, just return value
        if sqe.value is not None:
            return sqe.value

        # sqe is now detached
        sqe.parentSq = None

        # demarshal into user object if possible
        returnObj = sqe.demarshal(self.objSample)

        return returnObj

    # looks up elem from user object or else raises ObjectNotRecognized
    def __lookup_elem(self, obj):
        if isinstance(obj, superqelem):
            name = obj.name
        elif self.keyCol is not None:
            try:
                name = getattr(obj, self.keyCol)
            except:
                raise ObjectNotRecognized('keyCol not found')
        else:
            try:
                name = obj._superqelemKey
            except:
                raise ObjectNotRecognized('no superqelemKey')

        return self.__internalDict[name]

    def buildFromStr(self, sqStr, attach = False):
        # initialize internal storage
        self.__internalList = LinkedList()
        self.__internalDict = {}

        # separate out sq header from remainder
        headerSeparatorIdx = sqStr.index(';')
        sqHeader = sqStr[ : headerSeparatorIdx]
        sqStr = sqStr[headerSeparatorIdx + 1 : ]

        # get name and number of fields from sq header
        headerElems = sqHeader.split(',')
        self.name = headerElems[0]
        numSqes = int(headerElems[1])

        # separate out attributes from remainder
        headerSeparatorIdx = sqStr.index(';')
        sqAttrs = sqStr[ : headerSeparatorIdx]
        sqStr = sqStr[headerSeparatorIdx + 1 : ]

        # set attributes
        attrElems = sqAttrs.split(',')
        for attr in attrElems:
            name, value = attr.split('|')

            if value.startswith('None'):
                value = None

            setattr(self, name, value)

        if attach:
            self.attach()

        # parse out each superqelem
        for i in range(0, numSqes):
            # separate field length indicator from remainder
            separatorIdx = sqStr.index(',')
            elemLen = int(sqStr[ : separatorIdx])
            sqStr = sqStr[separatorIdx + 1 : ]

            # slice the rest of the sqe out
            sqeStr = sqStr[ : elemLen]
            sqStr = sqStr[elemLen : ]

            # deserialize sqe from string fragment
            sqe = superqelem(sqeStr, parentSq = self, buildFromStr = True)

            # add element to internal dictionary and tail of internal list
            self.__internalDict[sqe.name] = sqe
            self.__internalList.push_tail(sqe)

    def buildFromFile(self, fileName, attach = False):
        with open(fileName) as infile:
            sqHdr = infile.readline().rstrip()

            self.name = sqHdr

            sqAttrs = infile.readline().rstrip()

            # set attributes
            attrElems = sqAttrs.split(',')
            for attr in attrElems:
                name, value = attr.split('|')

                if value.startswith('None'):
                    value = None

                setattr(self, name, value)

            if attach:
                self.attach()

            for line in infile:
                # deserialize sqe from string fragment
                sqe = superqelem(line, parentSq = self, buildFromStr = True)

                # add sqe to sq
                self.push(sqe)

    def save(self, fileName):
        with open(fileName, 'w') as f:
            sqHdr = '{0}'.format(self.name)
            f.write('{0}\n'.format(sqHdr))

            # serialize relevant attributes as name-value pairs
            sqAttrs = ''
            sqAttrs += 'host|{0},'.format(self.host)
            sqAttrs += 'keyCol|{0},'.format(self.keyCol)
            sqAttrs += 'maxlen|{0},'.format(self.maxlen)
            sqAttrs += 'autoKey|{0}'.format(self.autoKey)

            f.write('{0}\n'.format(sqAttrs))

            for sqe in self.__internalList:
                f.write('{0}\n'.format(str(sqe)))

    # returns superqelems without any attempt at demarshalling
    def n(self, key):
        if key in self.__internalDict:
            return self.__internalDict[key]
        elif isinstance(key, int) and key < len(self.__internalDict):
            # if element isn't keyed on the int, try the int as an index
            return self.__internalList[key]                      
        else:
            raise KeyError(key)

    # returns internal list
    def _list(self):
        return self.__internalList

    # return internal list as python list
    def list(self):
        return [val for val in self]

    def dict(self):
        return self.__internalDict

    def attach(self):
        if self.attached:
            raise Exception('Already attached!')

        if self.dataStore.superq_exists(self.name):
            raise NotImplemented('Not yet allowed to attach existing superqs.')

        self.attached = True

        self.dataStore.superq_create(self)

        # if attaching a locally-backed superq, back each elem
        if self.host is None or self.dataStore.public:
            # create each elem. The 1st one triggers backing table creation
            for sqe in self.__internalList:
                self.create_elem_datastore_only(sqe)

    def detach(self):
        if not self.attached:
            raise Exception('Not attached!')

        self.attached = False

    def reload(self):
        raise NotImplemented(superq.reload())

    def query(self, colLst, tableLst, conditionalStr, objSample = None):
        if not self.attached:
            raise NotImplemented('queries not supported on detached superqs')

        return self.dataStore.superq_query(self,
                                           colLst,
                                           tableLst,
                                           conditionalStr,
                                           objSample)

    def update(self):
        raise NotImplemented(superq.update())

    def delete(self):
        if self.attached:
            self.attached = False
            self.dataStore.superq_delete(self)

    # inspect first sqe to determine backing table characteristics
    def __initialize_on_first_elem(self, sqe):
        self.nameStr = ''
        self.nameTypeStr = ''

        # scalar superqelem
        if sqe.value is not None:
            self.nameStr = '_name_,_val_,_links_'

            self.nameTypeStr = '_name_ TEXT,'
            if sqe.valueType.startswith('str'):
                self.nameTypeStr += '_val_ TEXT'
            elif sqe.valueType.startswith('int'):
                self.nameTypeStr += '_val_ INTEGER'
            elif sqe.valueType.startswith('float'):
                self.nameTypeStr += '_val_ REAL'

            # special _links_ column
            self.nameTypeStr += ',_links_ TEXT'

            self.colNames = ['_name_', '_val_', '_links_']
            self.colTypes = ['str', sqe.valueType, 'str']
            
            return

        colNames = []
        colTypes = []

        # support autoKey
        if self.keyCol is None:
            self.nameStr = '_name_,'
            self.nameTypeStr = '_name_ TEXT,'
            colNames = ['_name_']
            colTypes = ['str']

        # non-scalar superqelem
        for atom in sqe:
            if atom.type.startswith('str'):
                self.nameTypeStr += '{0} TEXT,'.format(atom.name)
            elif atom.type.startswith('int'):
                self.nameTypeStr += '{0} INTEGER,'.format(atom.name)
            elif atom.type.startswith('float'):
                self.nameTypeStr += '{0} REAL,'.format(atom.name)
            else:
                raise TypeError('Unsupported type {0}'.format(atom.type))

            colTypes.append(atom.type)

            # add column name to list of names and name string
            colNames.append(atom.name)
            self.nameStr += '{0},'.format(atom.name)

        # strip trailing commas
        self.nameStr = self.nameStr.rstrip(',')
        self.nameTypeStr = self.nameTypeStr.rstrip(',')

        # append special _links_ column info
        self.nameStr += ',_links_'
        self.nameTypeStr += ',_links_ TEXT'
        colNames.append('_links_')
        colTypes.append('str')

        self.colNames = colNames
        self.colTypes = colTypes

    def create_elem_datastore_only(self, sqe, idx = None):
        # enable sqe to trigger datastore updates through parent sq
        sqe.parentSq = self

        # build understanding of backing table the 1st time through
        if not self.colNames:
            # build understanding of object structure
            self.__initialize_on_first_elem(sqe)

            # set flag to create table if non-hosted or dataStore is public
            if self.host is None or self.dataStore.public:
                self.createTable = True

        if self.attached:
            self.dataStore.superqelem_create(self, sqe, idx, self.createTable)

            if self.createTable:
                self.createTable = False

    def create_elem(self, value, name = None, idx = None):
        return self.push(self.__wrap_elem(value, name), idx)

    def read_elem(self, key = None, idx = None):
        if key is not None:
            return self[key]
        elif idx is not None:
            return self.__internalList[idx]
        else:
            # efficiently return random element without list traversal
            keys = self.__internalDict.keys()
            return self.__internalDict[keys[random.randrange(0, len(keys))]]

    # exists for sqe.__setProperty() to update datastore without recursing
    def update_elem_datastore_only(self, sqe):
        if self.attached:
            self.dataStore.superqelem_update(self, sqe)

    def update_elem(self, value):
        if isinstance(value, superqelem):
            sqe = value

            if sqe.parentSq is None:
                attachedSqe = self.__internalDict[sqe.name]

                # handle scalars
                attachedSqe.set_scalar(sqe.value)

                # 'demarshal' from detached sqe to attached
                for atom in attachedSqe:
                    atom.value = sqe[atom.name]

                # rebuild links
                attachedSqe.resetLinks()
                attachedSqe.addLinksFromStr(sqe.links)
# TODO: I'm feeling update_elem_datastore_only() should happen here too
            else:
                self.update_elem_datastore_only(sqe)

            return

        # lookup sqe from user object
        sqe = self.__lookup_elem(value)
            
        # 'marshal' from user object to sqe
        for atom in sqe:
            atom.value = getattr(value, atom.name)

        self.update_elem_datastore_only(sqe)

    def delete_elem_datastore_only(self, sqe):
        if self.attached:
            self.dataStore.superqelem_delete(self, sqe.name)

    def delete_elem(self, value):
        if isinstance(value, superqelem):
            sqe = value
        elif isinstance(value, (str, int, float)) and \
             value in self.__internalDict:
            sqe = self.__internalDict[value]
        elif isinstance(value, int) and value < len(self.__internalDict):
            sqe = self.__internalList[value]
        else:
            # lookup sqe from user object
            sqe = self.__lookup_elem(value)

        with self.not_empty:
            # remove element from internal collections
            self.__internalDict.pop(sqe.name)
            self.__internalList.pop_node(sqe)

            self.delete_elem_datastore_only(sqe)

            self.not_full.notify()

    # these thread-safe methods can be used for synchronized superq access

    def push(self, value, idx = None, block = True, timeout = None):
        with self.not_full:
            # handle dropping an element if needed
            if self.maxlen is not None and len(self) > self.maxlen:
                if block:
                    if timeout is None:
                        while len(self) >= self.maxlen:
                            self.not_full.wait()
                    elif timeout < 0:
                        raise ValueError('timeout must be non-negative')
                    else:
                        endtime = time() + timeout
                        while len(self) >= self.maxlen:
                            remaining = endtime - time()
                            if remaining <= 0.0:
                                raise SuperQFull('superq is full')
                            self.not_full.wait(remaining)
                else:
                    if self.maxlen < 0:
                        raise ValueError('maxlen is negative')
                    return
            elif self.maxlen is not None and len(self) == self.maxlen:
                if idx is None or idx >= len(self) - 1:
                    self.pop_head()
                elif idx <= 0:
                    self.pop_tail()
                else:
                    raise ValueError('Cannot insert into full set')

            # convert value to sqe if necessary
            sqe = self.__wrap_elem(value)

            # add sqe to internal dictionary
            self.__internalDict[sqe.name] = sqe

            # add sqe to internal list
            if idx is None or idx >= len(self) - 1:
                # default to stack/LIFO behavior
                self.__internalList.push_tail(sqe)
            elif idx == 0:
                self.__internalList.push_head(sqe)
            else:
                self.__internalList.push(idx, sqe)

            # for now pushes on hosted superqs are slow due to blocking here
            if self.attached:
                self.create_elem_datastore_only(sqe, idx)

            self.not_empty.notify()

            # return the object for elegant create_elem()
            return sqe

    def push_head(self, value, block = True, timeout = None):
        return self.push(value, 0, block, timeout)

    def push_tail(self, value, block = True, timeout = None):
        return self.push(value, len(self), block, timeout)

    def pop(self, idx = None, block = True, timeout = None):
        with self.not_empty:
            if not block:
                if len(self) == 0:
                    raise SuperQEmpty('no elements in superq')
            elif timeout is None:
                while len(self) == 0:
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError('timeout must be non-negative')
            else:
                endtime = time() + timeout
                while len(self) == 0:
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise SuperQEmpty
                    self.not_empty.wait(remaining)

            # default to stack/LIFO behavior
            if idx is None:
                idx = len(self) - 1

            # remove element from internal collections
            sqe = self.__internalList.pop(idx)
            self.__internalDict.pop(sqe.name)

            # for now pops on hosted superqs are slow due to blocking here
            if self.attached:
                self.delete_elem_datastore_only(sqe)

            self.not_full.notify()

            return self.__unwrap_elem(sqe)

    def pop_head(self, block = True, timeout = None):
        return self.pop(0, block, timeout)

    def pop_tail(self, block = True, timeout = None):
        return self.pop(len(self) - 1, block, timeout)

    # rotate superqelems n steps to the right. If n is negative, rotates left
    def rotate(self, n):
        # iterate to the indicated index
        if n >= 0:
            for i in range(0, n):
                self.push_head(self.pop_tail())
        else:
            for i in range(1, abs(n)):
                self.push_tail(self.pop_head())

    # waits for superq to be empty
    def join(self):
        raise NotImplemented('superq.join()')

# create public network node instance or private instance for program
_dataStore = SuperQDataStore()

_nodeRequestNextId = 1
_nodeRequestLock = Lock()
class SuperQNodeRequest():
    def __init__(self, msg_id_ = '', cmd_ = 0, args_ = '', body_ = ''):
        self.msg_id = msg_id_
        self.cmd = cmd_
        self.args = args_
        self.body = body_

        if self.msg_id == '':
            self.__set_msg_id()

    def __set_msg_id(self):
        global _nodeRequestNextId
        global _nodeRequestLock

        _nodeRequestLock.acquire()
        self.msg_id = str(_nodeRequestNextId)
        _nodeRequestNextId += 1
        _nodeRequestLock.release()

    def __str__(self):
        return '{0}|{1}|{2}%{3}'.format(self.msg_id,
                                         self.cmd,
                                         self.args,
                                         self.body)

    def from_str(self, requestStr):
        try:  
            headerSeparatorIdx = requestStr.index('%')

            # separate out cmd header and body
            cmdHeader = requestStr[ : headerSeparatorIdx]
            body = requestStr[headerSeparatorIdx + 1 : ]

            elems = cmdHeader.split('|')

            if len(elems) != 3:
                raise MalformedNetworkRequest(requestStr)

            self.msg_id = elems[0]
            self.cmd = SQNodeCmd(int(elems[1]))
            self.args = elems[2]
            self.body = body
        except Exception as e:
            exceptStr = 'Request: {0}\nException: {1}'.format(requestStr, e)
            raise MalformedNetworkRequest(exceptStr)

class SuperQNodeResponse():
    def __init__(self, msg_id_ = '', result_ = '', body_ = ''):
        self.msg_id = msg_id_
        self.result = result_
        self.body = body_

    def __str__(self):
        return '{0}|{1}%{2}'.format(self.msg_id,
                                    self.result,
                                    self.body)

    def from_str(self, responseStr):
        try:
            headerSeparatorIdx = responseStr.index('%')
            
            # separate out cmd header and body
            responseHeader = responseStr[ : headerSeparatorIdx]
            responseBody = responseStr[headerSeparatorIdx + 1 : ]

            elems = responseHeader.split('|')

            if len(elems) != 2:
                raise MalformedNetworkResponse(responseStr)

            self.msg_id = elems[0]
            self.result = elems[1]
            self.body = responseBody
        except Exception as e:
            exceptStr = 'Response: {0}\nException: {1}'.format(responseStr, e)
            raise MalformedNetworkResponse(exceptStr)

# manages network connections and requests to network nodes
class SuperQNetworkClientMgr():
    def __init__(self):
        # client is currently responsible for starting up network node
        self.__nodeProcess = None

        self.__nodeProcessLock = Lock()

        # dictionary of superq-based socket pools keyed by (host, port)
        self.__socketPoolDict = {}

    def __start_networked_datastore(self):
        # start superq local network node
        with self.__nodeProcessLock:
            if self.__nodeProcess is not None:
                return

            nodeArgs = ['python3',
                        'superq.py',
                        '-t',
                        str(DEFAULT_TCP_PORT),
                        '-s',
                        str(DEFAULT_SSL_PORT)]

            if WIN32_POPEN_FLAGS is not None:
                self.__nodeProcess = Popen(nodeArgs,
                                           creationflags = WIN32_POPEN_FLAGS)
            else:
                self.__nodeProcess = Popen(nodeArgs)

            with open('node.pid', 'w') as f:
                f.write(str(self.__nodeProcess.pid))

    def shutdown(self):
        if self.__nodeProcess is not None:
            kill(self.__nodeProcess.pid, 9)

        # cleanup socket pools
        for key, socketPool in self.__socketPoolDict.items():
            while True:
                try:
                    s = socketPool.pop(block = False)
                except SuperQEmpty:
                    break
                else:
                    s.close()

    def __new_socket(self,
                     host = 'localhost',
                     port = DEFAULT_TCP_PORT,
                     ssl = False):
        s = socket(AF_INET, SOCK_STREAM)

        # convert socket to ssl if requested
        if ssl:
            s = wrap_socket(s,
                            ca_certs = DEFAULT_SSL_PEM_FILE,
                            cert_reqs = CERT_REQUIRED,
                            ssl_version = PROTOCOL_TLSv1)

        try:
            s.connect((host, port))
            return s
        except ConnectionRefusedError:
            # start localhost datastore
            if host == 'localhost' and port == DEFAULT_TCP_PORT:
                self.__start_networked_datastore()

                # attempt to connect to datastore once it is started
                attempts = 0
                maxAttempts = 5
                while attempts < maxAttempts:
                    try:
                        s.connect((host, port))
                        return s
                    except:
                        sleep(.2)
                    attempts += 1

            raise

    def __get_socket(self, host, port, ssl = False):                                
        # get or initialize socket pool specific to host and port
        try:
            socketPool = self.__socketPoolDict[(host, port)]            
        except KeyError:
            # create detached superq to use for socket pool
            socketPool = superq([])
            self.__socketPoolDict[(host, port)] = socketPool

        # get existing socket if available or open new one
        try:
            s = socketPool.pop(block = False)
        except SuperQEmpty:
            return self.__new_socket(host, port, ssl)

        return s

    def __return_socket(self,
                        s,
                        host = 'localhost',
                        port = DEFAULT_TCP_PORT):
        # return socket to appropriate socket pool
        self.__socketPoolDict[(host, port)].push(s)

    def __send(self, s, msg):
        buffer = msg
        
        totalSent = 0
        while totalSent < len(buffer):
            bytesSent = s.send(buffer[totalSent:])
            if bytesSent == 0:
                raise RuntimeError('Connection closed.')
            totalSent = totalSent + bytesSent

    def __recv(self, s, bytesToRecv):
        buffer = bytearray()

        while len(buffer) < bytesToRecv:
            tempBuf = s.recv(bytesToRecv - len(buffer))
            if len(tempBuf) == 0:
                raise RuntimeError('Connection closed.')
            buffer += tempBuf
            
        return buffer

    def __get_msg(self, s):
        # first byte will always be a marker to verify begining of Request
        data = self.__recv(s, 1)

        if (len(data) == 0 or data[0] != SUPERQ_MSG_HEADER_BYTE):
            raise Exception('Bad message.')

        # next 4 bytes must always be message body length
        data = bytearray()
        while len(data) < 4:
            currentData = self.__recv(s, 4 - len(data))

            if len(currentData) == 0:
                raise Exception('0 bytes read. Connection probably closed.')

            data += currentData

        # convert length
        messageLength = unpack('I', data)[0]

        # now read the rest of the message
        data = bytearray()
        while len(data) < messageLength:
            currentData = self.__recv(s, messageLength - len(data))

            if len(currentData) == 0:
                raise RuntimeError('Connection closed.')

            data += currentData

        # decode character data
        msg = data.decode('utf-8')

        # build response object from string
        response = SuperQNodeResponse()
        response.from_str(msg)

        return response

    def __send_msg(self, host, strMsg):
        ssl = False

        # 'local' is shorthand for localhost:DEFAULT_PORT
        if host == 'local':
            host = 'localhost'
            port = DEFAULT_TCP_PORT
        else:
            if host.startswith('ssl:'):
                ssl, host, port = host.split(':')
                ssl = True
                port = int(port)
            else:
                try:
                    host, port = host.split(':')
                    port = int(port)
                except ValueError:
                    port = DEFAULT_TCP_PORT

        msg = bytearray()
        msg.append(SUPERQ_MSG_HEADER_BYTE)
        msg.extend(pack('I', len(strMsg)))
        msg.extend(strMsg.encode('utf-8'))

        # get existing socket from socket pool or initialize new one
        s = self.__get_socket(host, port, ssl)

        # send message
        self.__send(s, msg)

        # get response
        response = self.__get_msg(s)

        # return socket to thread pool
        self.__return_socket(s, host, port)

        return response

    # this might be used in the case of create_elem for instance, to provide
    #  a non-blocking operation. But it requires some kind of transactional
    #  implementation or solution to prevent synchronization errors
    def __send_msg_async(self, host, strMsg):
        t = Thread(target = self.__send_msg, args = (host, strMsg))
        t.start()

    def superq_exists(self, name, host):
        # build request object from string
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superq_exists.value
        request.args = name

        response = self.__send_msg(host, str(request))

        return eval(response.result)

    def superq_create(self, sq):
        # build request object from string
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superq_create.value
        request.args = sq.publicName
        request.body = str(sq)

        response = self.__send_msg(sq.host, str(request))

        if not eval(response.result):
            raise SuperQEx('superq_create(): {0}'.format(response))

    def superq_read(self, name, host):
        # build request object from string
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superq_read.value
        request.args = name

        response = self.__send_msg(host, str(request))

        if not eval(response.result):
            raise SuperQEx('superq_read(): {0}'.format(response))

        # deserialize response body into a detached superq
        sq = superq(response.body, attach = False, buildFromStr = True)

        return sq

    def superq_delete(self, sq):
        # build request object
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superq_delete.value
        request.args = sq.publicName

        response = self.__send_msg(sq.host, str(request))

        if not eval(response.result):
            raise SuperQEx('superq_delete(): {0}'.format(response))

    def superq_query(self, sq, queryStr):
        # build request object from string
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superq_query.value
        request.args = sq.publicName
        request.body = queryStr

        response = self.__send_msg(sq.host, str(request))

        if eval(response.result):
            return superq(response.body, attach = False, buildFromStr = True)
        else:
            raise SuperQEx('superq_query(): {0}'.format(response))

    def superqelem_create(self, sq, sqe, idx = None):
        # build request object
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superqelem_create.value
        request.args = '{0},{1}'.format(sq.publicName, idx)
        request.body = str(sqe)

        response = self.__send_msg(sq.host, str(request))

        if not eval(response.result):
            raise SuperQEx('superqelem_create(): {0}'.format(str(response)))

    def superqelem_update(self, sq, sqe):
        # build request object
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superqelem_update.value
        request.args = '{0}'.format(sq.publicName)
        request.body = str(sqe)

        response = self.__send_msg(sq.host, str(request))

        if not eval(response.result):
            raise SuperQEx('superqelem_update(): {0}'.format(str(response)))

    def superqelem_delete(self, sq, sqeName):
        # build request object
        request = SuperQNodeRequest()
        request.cmd = SQNodeCmd.superqelem_delete.value
        request.args = '{0}'.format(sq.publicName)
        request.body = '{0}'.format(sqeName)

        response = self.__send_msg(sq.host, str(request))

        if not eval(response.result):
            raise SuperQEx('superqelem_delete(): {0}'.format(str(response)))

# deserializes requests, processes them, and serializes responses
class SuperQStreamHandler(StreamRequestHandler):
    def handle(self):             
        # client can stay connected for multiple Request-Response transactions
        while True:
            try:
                self.handle_connection()
            except Exception as e:
                tb = format_exc()
                self.raise_error('Exception: {0}\nTrace: {1}'.format(e, tb))
        self.request.close()

    def raise_error(self, msg):
        with open('node.output', 'a') as f:
            f.write('\n' + msg)

        raise RuntimeError(msg)

    def return_response(self, response):
        strResponse = str(response)
        msg = bytearray()
        
        msg.append(SUPERQ_MSG_HEADER_BYTE)
        msg.extend(pack('I', len(strResponse)))
        msg.extend(strResponse.encode('utf-8'))

        self.wfile.write(msg)

    def handle_connection(self):
        # first byte will always be a marker to verify beginning of Request
        data = self.connection.recv(1)

        if len(data) == 0:
            self.raise_error('connection closed while reading marker')

        if data[0] != SUPERQ_MSG_HEADER_BYTE:
            self.raise_error('invalid marker ({0}).'.format(data[0]))

        # next 4 bytes must always be message body length
        data = bytearray()
        try:
            while len(data) < 4:
                currentData = self.connection.recv(4 - len(data))

                if len(currentData) == 0:
                    self.raise_error('connection closed while reading length')

                data += currentData
        except Exception as e:
            self.raise_error(str(e))
            raise

        # convert length
        messageLength = unpack('I', data)[0]

        # now read the rest of the message
        data = bytearray()
        while len(data) < messageLength:
            currentData = self.connection.recv(messageLength - len(data))

            if len(currentData) == 0:
                self.raise_error('connection closed during read')

            data += currentData

        # decode character data
        msg = data.decode('utf-8')

        # build request object from string
        request = SuperQNodeRequest()
        request.from_str(msg)

        # start building response
        response = SuperQNodeResponse()
        response.msg_id = request.msg_id
        response.result = str(False)

        cmd = request.cmd
        args = request.args
        body = request.body

        if cmd == SQNodeCmd.superq_exists:
            response.result = str(_dataStore.superq_exists(args))
            response.body = ''
        elif cmd == SQNodeCmd.superq_create:
            if _dataStore.superq_exists(args):
                response.result = str(False)
            else:
                # deserialize request body into a detached superq
                sq = superq(body, attach = False, buildFromStr = True)

                # assign superq to the node datastore
                sq.attach()

                response.result = str(True)
        elif cmd == SQNodeCmd.superq_read:
            sq = _dataStore.superq_read(args)

            response.body = str(sq)

            response.result = str(True)
        elif cmd == SQNodeCmd.superq_delete:
            try:
                sq = _dataStore.superq_read(args)
            except:
                raise KeyError('superq {0} does not exist'.format(args))

            _dataStore.superq_delete(sq)

            response.result = str(True)
        elif cmd == SQNodeCmd.superq_query:
            try:
                sq = _dataStore.superq_read(args)
            except:
                raise KeyError('superq {0} does not exist'.format(args))

            # store resulting superq in response body
            response.body = str(_dataStore.superq_query_local(body))

            response.result = str(True)
        elif cmd == SQNodeCmd.superqelem_exists:
            pass
        elif cmd == SQNodeCmd.superqelem_create:
            sqName, sqeIdx = args.split(',')

            try:
                sqeIdx = int(sqeIdx)
            except ValueError:
                sqeIdx = None

            try:
                sq = _dataStore.superq_read(sqName)
            except KeyError:
                raise KeyError('superq {0} does not exist'.format(sqName))

            # build sqe from request
            sqe = superqelem(body, buildFromStr = True)

            sq.create_elem(sqe, idx = sqeIdx)

            response.result = str(True)
        elif cmd == SQNodeCmd.superqelem_read:
            pass
        elif cmd == SQNodeCmd.superqelem_update:
            sqName = args

            try:
                sq = _dataStore.superq_read(sqName)
            except KeyError:
                raise KeyError('superq {0} does not exist'.format(sqName))

            # build sqe from request
            sqe = superqelem(body, buildFromStr = True)

            sq.update_elem(sqe)

            response.result = str(True)
        elif cmd == SQNodeCmd.superqelem_delete:
            sqName = args
            sqeName = body

            try:
                sq = _dataStore.superq_read(sqName)
            except KeyError:
                raise KeyError('superq {0} does not exist'.format(sqName))

            sq.delete_elem(sqeName)

            response.result = str(True)
        else:
            raise MalformedNetworkRequest(msg)

        self.return_response(response)
          
class SuperQTCPServer(TCPServer):
    def __init__(self,
                 server_address,
                 RequestHandlerClass,
                 bind_and_activate = True):
        TCPServer.__init__(self,
                           server_address,
                           RequestHandlerClass,
                           bind_and_activate)

    def get_request(self):
        newsocket, fromaddr = self.socket.accept()

        return newsocket, fromaddr

class SuperQTCPThreadedServer(ThreadingMixIn, SuperQTCPServer): pass

class SuperQSSLServer(TCPServer):
    def __init__(self,
                 server_address,
                 RequestHandlerClass,
                 certfile,
                 keyfile,
                 ssl_version = PROTOCOL_TLSv1,
                 bind_and_activate = True):
        TCPServer.__init__(self,
                           server_address,
                           RequestHandlerClass,
                           bind_and_activate)
        self.certfile = certfile
        self.keyfile = keyfile
        self.ssl_version = ssl_version

    def get_request(self):
        newsocket, fromaddr = self.socket.accept()

        connstream = ssl.wrap_socket(newsocket,
                                 server_side = True,
                                 certfile = self.certfile,
                                 keyfile = self.keyfile,
                                 ssl_version = self.ssl_version)
        
        return connstream, fromaddr

class SuperQSSLThreadedServer(ThreadingMixIn, SuperQSSLServer): pass

# provides local and remote network interfaces for networked data store
class SuperQNetworkNode():
    def __init__(self):
        self.__tcpServer = None
        self.__sslServer = None

        self.__tcpThread = None
        self.__sslThread = None

    def launch_tcp_server(self, host, port):
        # create localhost TCP server on the given port
        self.__tcpServer = SuperQTCPThreadedServer((host, port),
                                                   SuperQStreamHandler)

        # handle requests until an explicit shutdown() request
        self.__tcpServer.serve_forever()

    def shutdown_tcp_server(self):
        self.__tcpServer.shutdown()
        self.__tcpThread.join()

        self.__tcpServer = None
        self.__tcpThread = None

    def launch_ssl_server(self, host, port):
        # create localhost SSL server on the given port
        self.__sslServer = SuperQSSLThreadedServer((host, port),
                                                   SuperQStreamHandler,
                                                   DEFAULT_SSL_PEM_FILE,
                                                   DEFAULT_SSL_KEY_FILE,
                                                   ssl_version = PROTOCOL_TLSv1)

        # handle requests until an explicit shutdown() request
        self.__sslServer.serve_forever()

    def shutdown_ssl_server(self):
        self.__sslServer.shutdown()
        self.__sslThread.join()

        self.__sslServer = None
        self.__sslThread = None

    def launch_node_mgr(self, tcpPort, sslPort, startSSL):
        log('Starting TCP connection handler ...')
        self.__tcpThread = Thread(target = self.launch_tcp_server,
                                  args = ('', tcpPort))
        self.__tcpThread.start()

        if startSSL:
            log('Starting SSL connection handler ...')
            self.__sslThread = Thread(target = self.launch_ssl_server,
                                      args = ('', sslPort))
            self.__sslThread.start()

    def shutdown_node(self):
        if self.__tcpServer:
            self.shutdown_tcp_server()

        if self.__sslServer:
            self.shutdown_ssl_server()

def main(argv):
    log('Starting superq public node ...')

    tcpPort = DEFAULT_TCP_PORT
    sslPort = DEFAULT_SSL_PORT

    sslEnabled = False

    try:
        opts, args = getopt(argv, 't:s:', ['tcpport=', 'sslport='])
    except GetoptError:
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-t', '--tcpport'):
            tcpPort = arg
        elif opt in ('-s', '--sslport'):
            sslEnabled = True
            sslPort = arg

    log('TCP port is {0}'.format(tcpPort))

    if (sslEnabled):
        log('SSL port is {0}. '.format(sslPort))

    log('Setting internal datastore to public ...')
    _dataStore.set_public()

    log('Creating and launching node ...')
    nodeMgr = SuperQNetworkNode()
    nodeMgr.launch_node_mgr(int(tcpPort), int(sslPort), sslEnabled)

    log('Cleaning up ...')
    nodeMgr.shutdown_node()

    log('Leaving main.')

if __name__ == '__main__':
    main(sys.argv[1:])
