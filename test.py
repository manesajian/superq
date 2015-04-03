import collections
import datetime
import random
import time

from os import remove
from superq import LinkedList, LinkedListNode, shutdown, superq, superqelem
from threading import Thread

class FooNode(LinkedListNode):
    def __init__(self, a):
        self.a = a

class Foo():
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def foo(self):
        return self.a

    def bar(self):
        return 42

class Foo2():
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c

class Foo3():
    def __init__(self, a, b):
        self.__dict__['a'] = a
        self.__dict__['b'] = b

    def __setattr__(self, attribute, value):
        if not attribute in self.__dict__:
            raise Exception('Adding attributes disabled.')
        else:
            self.__dict__[attribute] = value

try:
    print('\nLINKEDLIST tests:\n')

    print('Testing creating linked list ...')
    ll = LinkedList()

    print('Testing adding elements to tail ...')
    for i in range(0, 10):
        ll.push_tail(FooNode(i + 1))

    print('\tExpected list length = {0}, actual = {1}'.format(10, len(ll)))
    assert(len(ll) == 10)
    print('\tExpected value = {0}, actual = {1}'.format(1, ll[0].a))
    assert(ll[0].a == 1)

    print('Testing removing elements from head ...')
    for i in range(0, 5):
        ll.pop_head()
    print('\tExpected list length = {0}, actual = {1}'.format(5, len(ll)))
    assert(len(ll) == 5)

    print('Testing adding elements to head ...')
    for i in range(0, 5):
        ll.push_head(FooNode(i + 1))
    print('\tExpected list length = {0}, actual = {1}'.format(10, len(ll)))
    assert(len(ll) == 10)
    print('\tExpected value = {0}, actual = {1}'.format(5, ll[0].a))
    assert(ll[0].a == 5)

    print('Testing removing elements from tail ...')
    for i in range(0, 5):
        ll.pop_tail()
    print('\tExpected list length = {0}, actual = {1}'.format(5, len(ll)))
    assert(len(ll) == 5)

    print('Testing adding into middle of list ...')
    print('\tPush element into list ...')
    ll.push_middle(3, FooNode(11))
    print('\tExpected list length = {0}, actual = {1}'.format(6, len(ll)))
    assert(len(ll) == 6)
    print('\tExpected value = {0}, actual = {1}'.format(11, ll[3].a))
    assert(ll[3].a == 11)

    print('Testing removing from middle of list ...')
    print('\tPop element from list ...')
    fooNode = ll.pop_middle(3)
    print('\tExpected list length = {0}, actual = {1}'.format(5, len(ll)))
    assert(len(ll) == 5)
    print('\tExpected value = {0}, actual = {1}'.format(11, fooNode.a))
    assert(fooNode.a == 11)

    print('Testing push/pop from both sides of list ...')
    print('\tCreating linked list ...')
    ll = LinkedList()
    print('\tAdding elements ...')
    for i in range(0, 10):
        ll.push_tail(FooNode(i + 1))
    print('\tExpected list length = {0}, actual = {1}'.format(10, len(ll)))
    assert(len(ll) == 10)
    
    print('\nDETACHED superq tests:\n')

    print('Testing empty superq creation ...')
    sq = superq([])
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing empty superq creation from empty dictionary ...')
    sq = superq({})
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic str list ...')
    sq = superq(['1', '2', '3'])
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic int list ...')
    sq = superq([1, 2, 3])
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic float list ...')
    sq = superq([1.1, 2.1, 3.1])
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing retrieving element by index ...')
    sq = superq([4, 5, 6])
    sqe = sq[2]
    print('\tExpected value = {0}, received value = {1}'.format(6, sqe))
    assert(sqe == 6)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from non-unique list ...')
    sq = superq(['1', '1', '2'])
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing retrieving python list back from superq ...')
    pyLst = [1, 2, 3, 4, 5]
    sq = superq(pyLst)
    sqLst = sq.list()
    sqLstName = type(sqLst).__name__
    print('\tExpected type = {0}, actual = {1}'.format('list', sqLstName))
    assert(isinstance(sqLst, list))
    print('\tChecking values ...')
    for i in range(0, len(pyLst)):
        print('\t\tOriginal val: {0}, Current val: {1}'.format(pyLst[i],
                                                               sqLst[i]))
        assert(pyLst[i] == sqLst[i])       
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from dict ...')
    sq = superq({'1':'abc', '2':'xyz'})
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq basic slicing ...')
    print('\tCreating superq ...')
    sq = superq([10, 11, 12, 13, 14, 15])
    print('\tCopying superq via empty slice ...')
    sqCopy = sq[:]
    print('\tExpected superq length = {0}, actual = {1}'.format(6,
                                                                len(sqCopy)))
    assert(len(sqCopy) == 6)
    print('\tSlicing portion of copied superq ...')
    sqSlice = sqCopy[0:3]
    print('\tExpected superq length = {0}, actual = {1}'.format(3,
                                                                len(sqSlice)))
    assert(len(sqSlice) == 3)
    print('\tExpected values = {0}, {1}, actual = {2}, {3}'.format(10,
                                                                   12,
                                                                   sqSlice[0],
                                                                   sqSlice[2]))
    assert(sqSlice[0] == 10 and sqSlice[2] == 12)
    print('\tTesting [-1:] slice ...')
    sqSlice = sqSlice[-1:]
    print('\tExpected superq length = {0}, actual = {1}'.format(1,
                                                                len(sqSlice)))
    assert(len(sqSlice) == 1)
    print('\tExpected value = {0}, actual = {1}'.format(12, sqSlice[0]))
    assert(sqSlice[0] == 12)
    print('\tDeleting superq ...')
    sq.delete()

    print('Additional superq slicing ...')
    print('\tCreating superq ...')
    sq = superq([1, 2, 3, 4, 5, 6, 7, 8])
    print('\tTesting [1:4:2] slice ...')
    sqSlice = sq[1:4:2]
    print('\tExpected superq length = {0}, actual = {1}'.format(2,
                                                                len(sqSlice)))
    assert(len(sqSlice) == 2)
    print('\tExpected values = {0}, {1}, actual = {2}, {3}'.format(2,
                                                                   4,
                                                                   sqSlice[0],
                                                                   sqSlice[1]))
    assert(sqSlice[0] == 2 and sqSlice[1] == 4)
    print('\tTesting [::-1] reverse list slice ...')
    sqSlice = sq[::-1]
    print('\tExpected superq length = {0}, actual = {1}'.format(8,
                                                                len(sqSlice)))
    assert(len(sqSlice) == 8)
    print('\tExpected values = {0}, {1}, actual = {2}, {3}'.format(8,
                                                                   1,
                                                                   sqSlice[0],
                                                                   sqSlice[7]))
    assert(sqSlice[0] == 8 and sqSlice[7] == 1)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from custom object list ...')
    sq = superq([Foo(1,10), Foo(2,12), Foo(3,13)], keyCol = 'a')
    print('\tTesting reading elem by key ...')
    val = sq.read_elem(key = 2).a
    print('\tExpected value = {0}, received value = {1}'.format(2, val))
    assert(val == 2)
    print('\tTesting reading elem by idx ...')
    val = sq.read_elem(idx = 2).a          
    print('\tExpected value = {0}, received value = {1}'.format(3, val))
    assert(val == 3)
    print('\tDeleting superq ...')    
    sq.delete()

    print('Testing superq creation from custom object list (keyCol set)...')
    sq = superq([Foo('a', 1), Foo('b', 2)], keyCol = 'a')
    print('\tTesting creating superqelem ...')
    newSqe = sq.create_elem(Foo('c', 3))
    print('\tTesting reading value ...')
    val = sq[2].b
    print('\tExpected value = {0}, received value = {1}'.format(3, val))
    assert(val == 3)
    print('\tTesting deleting superqelem ...')
    sq.delete_elem(newSqe)
    print('\tExpected superq length = {0}, actual = {1}'.format(2, len(sq)))
    assert(len(sq) == 2)

    print('Testing retrieving original object from superq ...')
    val = sq[0].bar()
    print('\tExpected value = {0}, received value = {1}'.format(42, val))
    assert(val == 42)

    print('Testing superqelem iteration ...')
    sqe = sq.n(0)
    values = ['a', 1]
    valuesIdx = 0
    for atom in sqe:
        if atom.value != values[valuesIdx]:
            raise Exception('Val {0}; expecting {1}.'.format(atom.value,
                                                             values[valuesIdx]))
        valuesIdx += 1
    print('\tPassed value check.')

    print('Testing reading superqelem atom value by int index ...')
    atomVal = sqe[1]
    print('\tExpected value = {0}, actual = {1}'.format(1, atomVal))
    assert(atomVal == 1)

    print('Testing writing superqelem atom value by int index ...')
    sqe[1] = 5
    atomVal = sqe[1]
    print('\tExpected value = {0}, actual = {1}'.format(5, atomVal))
    assert(atomVal == 5)

    print('Testing attaching superq to datastore ...')
    sqName = sq.name
    sq.attach()
    sq = superq(sqName)
    print('\tExpected superq length = {0}, actual = {1}'.format(2, len(sq)))
    assert(len(sq) == 2)
    print('\tExpected value = {0}, actual = {1}'.format(2, sq[1].b))
    assert(sq[1].b == 2)
    print('\tDeleting superq ...')
    sq.delete()

    print('\nATTACHED superq tests:\n')

    print('Testing empty superq creation ...')
    sq = superq([], attach = True)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing empty superq creation from empty dictionary ...')
    sq = superq({}, attach = True)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic str list ...')
    sq = superq(['1', '2', '3'], attach = True)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic int list ...')
    sq = superq([1, 2, 3], attach = True)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic float list ...')
    sq = superq([1.1, 2.1, 3.1], attach = True)

    print('Testing modifying scalar superqelem by index ...')
    sq[2] = 5
    print('\tExpected value = {0}, actual = {1}'.format(5, sq[2]))
    assert(sq[2] == 5)

    print('Verifying superq deletion ...')
    sqName = sq.name
    sq.delete()
    try:
        sq = superq(sqName)
        raise Exception('\tExpected failure did not occur.')
    except:
        print('\tsuperq lookup correctly failed.')

    print('Testing datastore lookup of non-existent superq ...')
    try:
        sq2 = superq('sq2')
        raise Exception('\tExpected failure did not occur.')
    except:
        print('\tsuperq lookup correctly failed.')

    print('Testing superq creation from custom object list ...')
    lst = [Foo('a', 1), Foo('b', 2)]
    sq = superq(lst, keyCol = 'a', name = 'sq1', attach = True)

    print('Testing superq lookup from datastore ...')
    sq1 = superq('sq1', attach = True)

    print('Testing adding new superqelem ...')
    sq1.create_elem(Foo('c', 3))
    sqLen = len(superq('sq1', attach = True))
    print('\tExpected superq length = {0}, actual = {1}'.format(3, sqLen))
    assert(sqLen == 3)

    print('Testing modifying superqelem field ...')
    sq1.n(1).b = 5
    print('\tChecking modified superqelem field ...')
    val = superq('sq1')['b'].b
    print('\tExpected value = {0}, actual = {1}'.format(5, val))
    assert(val == 5)

    print('Testing looking up superqelem by index ...')
    sqe = superq('sq1').n(2)
    print('\tExpected name = {0}, actual = {1}'.format('c', sqe.name))
    assert(sqe.name == 'c')

    print('Testing retrieval of original object type ...')
    sq1 = superq('sq1')
    sq1.objSample = Foo('a', 1)
    myFoo = sq1[1]
    print('\tCalling function on object ...')
    val = myFoo.bar()
    print('\tExpected value = {0}, actual = {1}'.format(42, val))
    assert(val == 42)
    print('\tDeleting superq ...')
    sq1.delete()

    print('Testing support of basic data types ...')
    print('\tCreating superq ...')
    lst = [Foo2('aaa', 4, 1.1), Foo2('bbb', 5, 1.2)]
    sq1 = superq(lst, keyCol = 'a', name = 'sq1', attach = True)
    print('\tLooking up superq in datastore ...')
    sq1 = superq('sq1')
    print('\tExpected superq length = {0}, actual = {1}'.format(2, len(sq1)))
    assert(len(sq1) == 2)
    print('\tExpected value = {0}, actual = {1}'.format(1.2, sq1['bbb'].c))
    assert(sq1['bbb'].c == 1.2)
    print('\tDeleting superq ...')
    sq1.delete()

    print('Testing updating user object with valid keycol ...')
    print('\tCreating superq ...')
    lst = [Foo3('a', 1), Foo3('b', 2), Foo3('c', 3)]
    sq = superq(lst, keyCol = 'a', name = 'sq', attach = True)
    print('\tRetrieving user obj ...')
    foo3 = sq[0]
    print('\tModifying user obj value ...')
    foo3.b = 5
    print('\tAttempting to set unsettable object attribute ...')
    try:
        foo3.c = None
        raise Exception('Attribute set incorrectly succeeded.')
    except:
        print('\tAttribute set correctly failed.')
    print('\tUpdating superqelem from user obj ...')
    sq.update_elem(foo3)
    print('\tRetrieving superqelem value ...')
    val = superq('sq', attach = True)[0].b
    print('\tExpected value = {0}, actual = {1}'.format(5, val))
    assert(val == 5)
    
    print('Testing deleting user object with valid keycol ...')
    print('\tDeleting superqelem from user obj ...')
    sq.delete_elem(foo3)
    sqLen = len(superq('sq', attach = True))
    print('\tExpected superq length = {0}, actual = {1}'.format(2, sqLen))
    assert(sqLen == 2)    
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing attempting user object update with no keyCol ...')
    print('\tCreating superq ...')
    sq = superq([Foo3('a', 1), Foo3('b', 2), Foo3('c', 3)], name = 'sq', attach = True)
    print('\tRetrieving user obj ...')
    foo3 = sq[0]
    print('\tModifying user obj value ...')
    foo3.b = 5
    print('\tAttempting to set attribute on object that should be unsettable ...')
    try:
        foo3.c = None
        raise Exception('Attribute set incorrectly succeeded.')
    except:
        print('\tAttribute set correctly failed.')
    print('\tAttempting to update superqelem from user obj ...')
    try:
        sq.update_elem(foo3)
        raise Exception('Update incorrectly succeeded.')
    except:
        print('\tUpdate correctly failed.')
    
    print('Testing attempting user object delete with no keyCol ...')
    print('\tAttempting to delete superqelem from user obj ...')
    try:
        sq.delete_elem(foo3)
        raise Exception('Delete incorrectly succeeded.')
    except:
        print('\tDelete correctly failed.')
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing deleting superqelems keyed by different data types ...')
    print('\tCreating superq keyed by int ...')
    sq = superq([Foo(1, 'a'), Foo(2, 'b')], keyCol = 'a', name = 'sq', attach = True)
    print('\tDeleting superqelem ...')
    sq.delete_elem(2)
    sqLen = len(superq('sq'))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, sqLen))
    assert(sqLen == 1)
    print('\tDeleting superq ...')
    sq.delete()
    print('\tCreating superq keyed by str ...')
    sq = superq([Foo('ab', 1), Foo('bc', 2)], keyCol = 'a', name = 'sq', attach = True)
    print('\tDeleting superqelem ...')
    sq.delete_elem('bc')
    sqLen = len(superq('sq'))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, sqLen))
    assert(sqLen == 1)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing basic superq query ...')
    print('\tCreating new multi-element superq ...')
    myFoos = [Foo2('a', 1, .01),
              Foo2('b', 2, .02),
              Foo2('c', 3, .03),
              Foo2('d', 4, .04),
              Foo2('e', 5, .05),
              Foo2('f', 6, .06),
              Foo2('g', 7, .07),
              Foo2('h', 8, .08),
              Foo2('i', 9, .09),
              Foo2('j', 10, .1)]
    sqMulti = superq(myFoos, keyCol = 'a', name = 'sqMulti', attach = True)
    print('\tPerforming data sanity check ...')
    sqCheck = superq('sqMulti')
    print('\tExpected value = {0}, actual = {1}'.format(5, sqCheck['e'].b))
    assert(sqCheck['e'].b == 5)
    print('\tPerforming query for single result ...')
    sqResult = sqMulti.query(['a'], ['<self>'], 'b = {0}'.format(5))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    print('\tExpected value = {0}, actual = {1}'.format('e', sqResult[0]['a']))
    assert(sqResult[0]['a'] == 'e')

    print('Performing query for single integer result ...')
    sqResult = sqMulti.query(['b'], ['<self>'], 'c = {0}'.format('.05'))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    print('\tExpected value = {0}, actual = {1}'.format(5, sqResult[0]['b']))
    assert(sqResult[0]['b'] == 5)

    print('Performing query into custom object ...')
    sqResult = sqMulti.query(['a', 'b'], ['<self>'], 'c = {0}'.format('.1'))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    print('\tSetting objSample ...')
    sqResult.objSample = Foo('a', 1)
    myFoo = sqResult[0]
    print('\tExpected values = {0}, {1}, actual = {2}, {3}'.format('j', 10,
                                                                         myFoo.a, myFoo.b))
    assert(myFoo.a == 'j' and myFoo.b == 10)

    print('Performing multiple row query ...')
    sqResult = sqMulti.query(['a', 'b'], ['<self>'], 'c > {0}'.format(.02))
    print('\tExpected superq length = {0}, actual = {1}'.format(8, len(sqResult)))
    assert(len(sqResult) == 8)
    print('\tTesting attaching detached superq as new superq ...')
    sqResult.name = 'sqMulti2'
    sqResult.attach()
    sqCheck = superq('sqMulti2')
    print('\tExpected superq length = {0}, actual = {1}'.format(8, len(sqCheck)))
    assert(len(sqCheck) == 8)
    print('\tDeleting superqs ...')
    sqMulti.delete()
    sqCheck.delete()

    print('Testing basic join ...')
    print('\tCreating first superq ...')
    sqA = superq([Foo(1, 2), Foo(2, 3), Foo(3, 4)], keyCol = 'a', name = 'sqA', attach = True)
    print('\tCreating second superq ...')
    sqB = superq([Foo2('foo', 4, 1.5), Foo2('bar', 5, 2.5)], keyCol = 'a', name = 'sqB', attach = True)
    sampleFoo = Foo2(1, 1, 1.1)
    colLst = ['<self>.a', 'sqB.c']
    tableLst = ['<self>', 'sqB']
    conditionalStr = '<self>.b = sqB.b'
    print('\tPerforming join ...')
    sqResult = sqA.query(colLst, tableLst, conditionalStr, sampleFoo)
    print('\tExpected result length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    sqResult.objSample = sampleFoo
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(3, 1.5,
                                                                 sqResult[0].a,
                                                                 sqResult[0].c))
    assert(sqResult[0].a == 3 and sqResult[0].c == 1.5)
    print('\tDeleting superqs ...')
    sqA.delete()
    sqB.delete()

    print('Testing join returning multiple elements ...')
    print('\tCreating first superq ...')
    sqA = superq([Foo(1, 2), Foo(2, 3), Foo(3, 4)], keyCol = 'a', name = 'sqA', attach = True)
    print('\tCreating second superq ...')
    sqB = superq([Foo2('foo', 3, 1.5), Foo2('bar', 4, 2.5)], keyCol = 'a', name = 'sqB', attach = True)
    sampleFoo = Foo2(1, 1, 1.1)
    colLst = ['<self>.a', 'sqB.c']
    tableLst = ['<self>', 'sqB']
    conditionalStr = '<self>.b = sqB.b'
    print('\tPerforming join ...')
    sqResult = sqA.query(colLst, tableLst, conditionalStr, sampleFoo)
    print('\tExpected result length = {0}, actual = {1}'.format(2, len(sqResult)))
    assert(len(sqResult) == 2)
    sqResult.objSample = sampleFoo
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(3, 2.5,
                                                                        sqResult[1].a,
                                                                        sqResult[1].c))
    assert(sqResult[1].a == 3 and sqResult[1].c == 2.5)
    print('\tDeleting superqs ...')
    sqA.delete()
    sqB.delete()

    print('\nHOSTED superq tests:\n')

    print('Testing empty public superq creation ...')
    sq = superq([], attach = True, host = 'local')
    sqName = sq.name
    sq = superq(sqName, host = 'local')
    print('\tCreate succeeded.')
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic str list ...')
    sq = superq(['1', '2', '3'], attach = True, host = 'local')
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic int list ...')
    sq = superq([1, 2, 3], attach = True, host = 'local')
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from basic float list ...')
    sq = superq([1.1, 2.1, 3.1], attach = True, host = 'local')
    sq.delete()
    print('Testing deletion of empty superq ...')
    try:
        sq = superq(sqName, host = 'local')
        raise Exception('\tExpected failure did not occur.')
    except:
        print('\tsuperq lookup correctly failed.')

    print('Testing datastore lookup of non-existent superq ...')
    try:
        sq2 = superq('sq2', host = 'local')
        raise Exception('\tExpected failure did not occur.')
    except:
        print('\tsuperq lookup correctly failed.')

    print('Creating superq from basic scalar list ...')
    sq = superq(['1', '2', '3'], attach = True, host = 'local')
    sqName = sq.name

    print('Testing updating scalar list ...')
    sq[0] = '4'
    val = superq(sqName, host = 'local')[0]
    print('\tExpected value = {0}, actual = {1}'.format('4', val))
    assert(val == '4')
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing adding element to empty superq ...')
    print('\tCreating empty superq ...')
    sq = superq([], attach = True, host = 'local')
    sqName = sq.name
    print('\tAdding element to superq ...')
    sq['abc'] = Foo('a', 1)      
    print('\tExpected superq length = {0}, actual = {1}'.format(1, len(sq)))
    assert(len(sq) == 1)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing superq creation from custom object list ...')
    sq1 = superq([Foo('a', 1), Foo('b', 2)], keyCol = 'a', name = 'sq1', attach = True, host = 'local')

    print('Testing superq lookup from datastore ...')
    sq1 = superq('sq1', host = 'local', attach = True)

    print('Adding new superqelem ...')
    sq1.create_elem(Foo('c', 3))
    sqLen = len(superq('sq1', host = 'local', attach = True))
    print('\tExpected superq length = {0}, actual = {1}'.format(3, sqLen))
    assert(sqLen == 3)

    print('Re-attaching and adding additional superqelem ...')
    superq('sq1', host = 'local', attach = True).create_elem(Foo('d', 4))
    sqLen = len(superq('sq1', host = 'local', attach = True))
    print('\tExpected superq length = {0}, actual = {1}'.format(4, sqLen))
    assert(sqLen == 4)

    print('Modifying superqelem field ...')
    sq1.n('a').b = 5
    print('\tChecking modified superqelem field ...')
    val = superq('sq1', host = 'local')['a'].b
    print('\tExpected value = {0}, actual = {1}'.format(5, val))
    assert(val == 5)
    print('\tDeleting superq ...')
    sq1.delete()

    print('Testing basic superq query ...')
    print('\tCreating new multi-element superq ...')
    myFoos = [Foo2('a', 1, .01),
              Foo2('b', 2, .02),
              Foo2('c', 3, .03),
              Foo2('d', 4, .04),
              Foo2('e', 5, .05),
              Foo2('f', 6, .06),
              Foo2('g', 7, .07),
              Foo2('h', 8, .08),
              Foo2('i', 9, .09),
              Foo2('j', 10, .1)]
    sqMulti = superq(myFoos, keyCol = 'a', name = 'sqMulti', attach = True, host = 'local')
    print('\tPerforming data sanity check ...')
    sqCheck = superq('sqMulti', host = 'local')
    myFoo2 = Foo2('z', 100, 1.1)
    sqCheck.objSample = myFoo2
    print('\tExpected value = {0}, actual = {1}'.format(5, sqCheck['e'].b))
    assert(sqCheck['e'].b == 5)
    print('\tPerforming query for single result ...')
    sqResult = sqMulti.query(['a', 'b', 'c'], ['<self>'], 'b = {0}'.format(2))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    sqResult.objSample = myFoo2
    print('\tExpected value = {0}, actual = {1}'.format(.02, sqResult[0].c))
    assert(sqResult[0].c == .02)
    sqMulti.delete()

    print('Testing superq query returning superqelems ...')
    print('\tCreating new multi-element superq ...')
    myFoos = [Foo2('a', 1, .01),
              Foo2('b', 2, .02),
              Foo2('c', 3, .03),
              Foo2('d', 4, .04),
              Foo2('e', 5, .05)]
    sqMulti = superq(myFoos, keyCol = 'a', name = 'sqMulti', attach = True, host = 'local')
    print('\tPerforming data sanity check ...')
    sqCheck = superq('sqMulti', host = 'local')
    mySqe = superqelem()
    sqCheck.objSample = mySqe
    print('\tPerforming query for single result ...')
    sqResult = sqMulti.query(['a', 'b', 'c'], ['<self>'], 'b = {0}'.format(2))
    print('\tExpected superq length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    floatVal = float(sqResult[0].c)
    print('\tExpected value = {0}, actual = {1}'.format(.02, floatVal))
    assert(floatVal == .02)
    sqMulti.delete()
    
    print('Testing multi-element superq query ...')
    print('\tCreating new multi-element superq ...')
    myFoos = [Foo2('a', 1, .01),
              Foo2('b', 2, .02),
              Foo2('c', 3, .03),
              Foo2('d', 4, .04),
              Foo2('e', 5, .05),
              Foo2('f', 6, .06),
              Foo2('g', 7, .07),
              Foo2('h', 8, .08),
              Foo2('i', 9, .09),
              Foo2('j', 10, .1)]
    sqMulti = superq(myFoos, keyCol = 'a', name = 'sqMulti', attach = True, host = 'local')
    print('\tPerforming data sanity check ...')
    sqCheck = superq('sqMulti', host = 'local')
    print('\tExpected value = {0}, actual = {1}'.format(5, sqCheck['e'].b))
    assert(sqCheck['e'].b == 5)
    print('\tPerforming query for multiple results ...')
    sqResult = sqMulti.query(['a', 'b', 'c'], ['<self>'], 'b > {0}'.format(2))
    print('\tExpected superq length = {0}, actual = {1}'.format(8, len(sqResult)))
    assert(len(sqResult) == 8)
    sqMulti.delete()

    print('Testing basic join ...')
    print('\tCreating first superq ...')
    lstA = [Foo(1, 2), Foo(2, 3), Foo(3, 4)]
    lstB = [Foo2('foo', 4, 1.5), Foo2('bar', 5, 2.5)]
    sqA = superq(lstA, keyCol = 'a', name = 'sqA', attach = True, host = 'local')
    print('\tCreating second superq ...')
    sqB = superq(lstB, keyCol = 'a', name = 'sqB', attach = True, host = 'local')
    colLst = ['<self>.a', 'sqB.c']
    tableLst = ['<self>', 'sqB']
    conditionalStr = '<self>.b = sqB.b'
    print('\tPerforming join ...')
    sqResult = sqA.query(colLst, tableLst, conditionalStr, None)
    print('\tExpected result length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    sqResult.objSample = Foo2(1, 1, 1.1)
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(3, 1.5,
                                                                        sqResult[0].a,
                                                                        sqResult[0].c))
    assert(sqResult[0].a == 3 and sqResult[0].c == 1.5)
    print('\tDeleting superqs ...')
    sqA.delete()
    sqB.delete()

    print('Testing join returning similarly-named columns ...')
    print('\tCreating first superq ...')
    lstA = [Foo(1, 2), Foo(2, 3), Foo(3, 4)]
    lstB = [Foo2('foo', 4, 1.5), Foo2('bar', 5, 2.5)]
    sqA = superq(lstA, keyCol = 'a', name = 'sqA', attach = True, host = 'local')
    print('\tCreating second superq ...')
    sqB = superq(lstB, keyCol = 'a', name = 'sqB', attach = True, host = 'local')
    sampleFoo = Foo(1, 'a')
    colLst = ['<self>.a as a', 'sqB.a as b']
    tableLst = ['<self>', 'sqB']
    conditionalStr = '<self>.b = sqB.b'
    print('\tPerforming join ...')
    sqResult = sqA.query(colLst, tableLst, conditionalStr, sampleFoo)
    print('\tExpected result length = {0}, actual = {1}'.format(1, len(sqResult)))
    assert(len(sqResult) == 1)
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(3, 'foo',
                                                                 sqResult[0].a,
                                                                 sqResult[0].b))
    assert(sqResult[0].a == 3 and sqResult[0].b == 'foo')
    print('\tDeleting superqs ...')
    sqA.delete()
    sqB.delete()

    print('Testing join returning multiple elements ...')
    print('\tCreating first superq ...')
    lstA = [Foo(1, 2), Foo(2, 3), Foo(3, 4)]
    lstB = [Foo2('foo', 3, 1.5), Foo2('bar', 4, 2.5)]
    sqA = superq(lstA, keyCol = 'a', name = 'sqA', attach = True, host = 'local')
    print('\tCreating second superq ...')
    sqB = superq(lstB, keyCol = 'a', name = 'sqB', attach = True, host = 'local')
    sampleFoo = Foo2('str', 1, 1.1)
    colLst = ['<self>.a', 'sqB.c']
    tableLst = ['<self>', 'sqB']
    conditionalStr = '<self>.b = sqB.b'
    print('\tPerforming join ...')
    sqResult = sqA.query(colLst, tableLst, conditionalStr, sampleFoo)
    print('\tExpected result length = {0}, actual = {1}'.format(2, len(sqResult)))
    assert(len(sqResult) == 2)
    print('\tDeleting superqs ...')
    sqA.delete()
    sqB.delete()

    print('\nDEQUE basic functionality tests:\n')

    print('Creating hosted superq for deque tests ...')
    sq = superq([1, 2, 3, 4, 5], attach = True, host = 'local')
    print('\tChecking length ...')
    print('\tExpected length = {0}, actual = {1}'.format(5, len(sq)))
    assert(len(sq) == 5)

    print('Testing double-ended reads ...')
    print('\tReading from head ...')
    leftVal = sq[0]
    print('\tReading from tail ...')
    rightVal = sq[-1]
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(1, 5, leftVal, rightVal))
    assert(leftVal == 1)
    assert(rightVal == 5)

    print('Testing double-ended writes ...')
    print('\tWriting to head ...')
    sq.push_head(0)
    print('\tWriting to tail ...')
    sq.push_tail(6)
    print('\tExpected length = {0}, actual = {1}'.format(7, len(sq)))
    assert(len(sq) == 7)
    print('\tReading from head ...')
    leftVal = sq[0]
    print('\tReading from tail ...')
    rightVal = sq[-1]
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(0, 6, leftVal, rightVal))
    assert(leftVal == 0)
    assert(rightVal == 6)

    print('Testing doubled-ended pops ...')
    print('\tPopping from head ...')
    leftVal = sq.pop_head()
    print('\tPoppping from tail ...')
    rightVal = sq.pop_tail()
    print('\tExpected length = {0}, actual = {1}'.format(5, len(sq)))
    assert(len(sq) == 5)
    print('\tExpected values = {0},{1}, actual = {2},{3}'.format(0, 6, leftVal, rightVal))
    assert(leftVal == 0)
    assert(rightVal == 6)

    print('Testing inserting into middle ...')
    print('\tWriting to middle ...')
    sq.push(7, idx = 3)
    print('\tReading from middle ...')
    val = superq(sq.name, attach = True, host = 'local')[3]
    print('\tExpected length = {0}, actual = {1}'.format(6, len(sq)))
    assert(len(sq) == 6)
    print('\tExpected value = {0}, actual = {1}'.format(7, val))
    assert(val == 7)

    print('Deleting superq ...')
    sq.delete()

    print('\nMAXLEN functionality tests with hosted superqs:\n')

    print('Creating hosted superq for maxlen tests ...')
    sq = superq([1, 2, 3, 4, 5], host = 'local', maxlen = 5)
    print('\tChecking length ...')
    print('\tExpected length = {0}, actual = {1}'.format(5, len(sq)))
    assert(len(sq) == 5)

    print('Testing inserting element into middle of full set ...')
    try:
        sq.create_elem(6, idx = 2)
        raise Exception('\tExpected failure did not occur.')
    except:
        print('\tInserting elem correctly failed.')

    print('Testing inserting element into middle of non-full set ...')
    print('\tDropping elem ...')
    sq.delete_elem(sq.n(1).name)
    print('\tInserting elem ...')
    sq.create_elem(6, idx = 2)
    print('\tExpected length = {0}, actual = {1}'.format(5, len(sq)))
    assert(len(sq) == 5)

    print('Testing appending element to full set ...')
    print('\tAdding elem ...')
    sq.create_elem(7)
    print('\tExpected length = {0}, actual = {1}'.format(5, len(sq)))
    assert(len(sq) == 5)
    print('\tExpected value = {0}, actual = {1}'.format(3, sq[0]))
    assert(sq[0] == 3)

    print('Testing prepending element to full set ...')
    print('\tAdding elem ...')
    sq.create_elem(1, idx = 0)
    print('\tExpected length = {0}, actual = {1}'.format(5, len(sq)))
    assert(len(sq) == 5)
    print('\tExpected value = {0}, actual = {1}'.format(5, sq[len(sq) - 1]))
    assert(sq[len(sq) - 1] == 5)

    print('Testing increasing size of set ...')
    sq.maxlen += 1
    print('\tAdding elem ...')
    sq.create_elem(8)
    print('\tExpected length = {0}, actual = {1}'.format(6, len(sq)))
    assert(len(sq) == 6)
    print('\tExpected value = {0}, actual = {1}'.format(1, sq[0]))
    assert(sq[0] == 1)

# TODO: this test points to maxlen needing to be a property, so that when a
#  full set has its size decreased, it can remove elements from the head or
#  tail as appropriate. Possibly we can keep track of where the last element
#  was added in order to recognize direction. Presumably if direction is not
#  known, elements would be dropped from the tail by default. There could be
#  a setting like maxlen_trunc_head to change this behavior.
##    print('Testing decreasing size of set ...')
##    sq.maxlen -= 1
##    print('\tAdding elem ...')
##    sq.create_elem(9)
##    print('\tExpected length = {0}, actual = {1}'.format(6, len(sq)))
##    assert(len(sq) == 6)
##    print('\tExpected value = {0}, actual = {1}'.format(2, sq[0]))
##    assert(sq[0] == 2)

    print('Deleting superq ...')
    sq.delete()

    print('\nSAVE\RESTORE tests:\n')

    print('Testing basic save\\restore functionality ...')
    print('\tCreating hosted superq ...')
    myFoos = [Foo2('a', 1, .01),
              Foo2('b', 2, .02),
              Foo2('c', 3, .03),
              Foo2('d', 4, .04),
              Foo2('e', 5, .05)]
    sq = superq(myFoos, keyCol = 'a', name = 'sq', attach = True, host = 'local')
    print('\tSaving hosted superq ...')
    sq.save('superq_test.sq')
    print('\tDeleting hosted superq ...')
    sq.delete()
    print('\tRestoring hosted superq ...')
    sq = superq('superq_test.sq', attach = True, host = 'local', buildFromFile = True)
    print('\tChecking length of new superq ...')
    sqLen = len(superq('sq', host = 'local'))
    print('\tExpected length = {0}, actual = {1}'.format(5, sqLen))
    assert(sqLen == 5)
    print('\tDeleting superq ...')
    sq.delete()
    print('\tDeleting save file ...')
    remove('superq_test.sq')

    print('Additional larger test of save\\restore functionality ...')
    print('\tCreating hosted superq ...')
    sq = superq([], keyCol = 'a', name = 'sq', attach = True, host = 'local')
    for i in range(0, 1000):
        foo = Foo(i, i)
        sq.create_elem(foo)
    print('\tSaving hosted superq ...')
    sq.save('superq_test.sq')
    print('\tDeleting hosted superq ...')
    sq.delete()
    print('\tRestoring hosted superq ...')
    sq = superq('superq_test.sq', attach = True, host = 'local', buildFromFile = True)
    print('\tChecking length of new superq ...')
    sqLen = len(superq('sq', host = 'local'))
    print('\tExpected length = {0}, actual = {1}'.format(1000, sqLen))
    assert(sqLen == 1000)
    print('\tDeleting superq ...')
    sq.delete()
    print('\tDeleting save file ...')
    remove('superq_test.sq')

    print('\nSTRESS tests:\n')

    print('Creating hosted superq for maxlen tests ...')
    sq = superq([], attach = True, host = 'local')
    val = 1
    for i in range(0, 1000):
        foo = Foo(i, i)
        sq.create_elem(foo)
    print('\tChecking length ...')
    print('\tExpected length = {0}, actual = {1}'.format(1000, len(sq)))
    assert(len(sq) == 1000)
    print('\tDropping each superqelem ...')
    for elem in sq:
        sq.delete_elem(elem)
    print('\tDeleting superq ...')
    sq.delete()

    print('Testing join returning many elements ...')
    print('\tCreating first superq ...')
    sq1 = superq([], name = 'sq1', attach = True, host = 'local')
    val = 1
    for i in range(0, 1000):
        foo = Foo(i, i)
        sq1.create_elem(foo)
    print('\tChecking length ...')
    print('\tExpected length = {0}, actual = {1}'.format(1000, len(sq1)))
    assert(len(sq1) == 1000)
    print('\tCreating second superq ...')
    sq2 = superq([], name = 'sq2', attach = True, host = 'local')
    for i in range(0, 1000):
        foo = Foo(i, i)
        sq2.create_elem(foo)
    print('\tChecking length ...')
    print('\tExpected length = {0}, actual = {1}'.format(1000, len(sq2)))
    assert(len(sq2) == 1000)
    colLst = ['<self>.a', 'sq2.b']
    tableLst = ['<self>', 'sq2']
    conditionalStr = '<self>.a = sq2.b'
    print('\tPerforming join ...')
    sqResult = sq1.query(colLst, tableLst, conditionalStr, sampleFoo)
    print('\tExpected result length = {0}, actual = {1}'.format(1000, len(sqResult)))
    assert(len(sqResult) == 1000)
    print('\tDeleting superqs ...')
    sq1.delete()
    sq2.delete()

    print('\nTHREAD tests:\n')
    total_items = 2000
    producers = 10
    consumers = 10
    items_produced = 0
    items_consumed = 0
    def producer_thread(sqPending, producerIndex):
        global items_produced
        items_to_produce = total_items // producers
        for i in range(0, items_to_produce):
            val = (producerIndex * items_to_produce) + i
            sqPending.push(Foo(val, val))
            items_produced += 1

    def consumer_thread(sqPending, sqCompleted):
        global items_consumed
        while True:
            val = sqPending.pop()
            sqCompleted.push(val)
            items_consumed += 1
    
    print('Testing multi-Producer, multi-Consumer hosted superq ...')
    print('\tCreating pending jobs superq ...')
    sqPending = superq([], name = 'sqPending', attach = True, host = 'local')
    print('\tCreating completed jobs superq ...')
    sqCompleted = superq([], name = 'sqCompleted', attach = True, host = 'local')
    print('\tSpawning {0} consumers ...'.format(consumers))
    for i in range(0, consumers):
        thread = Thread(target = consumer_thread, args = (sqPending, sqCompleted))
        thread.daemon = True
        thread.start()
    print('\tSpawning {0} producers ...'.format(producers))
    producersStart = time.time()
    consumersStart = time.time()
    for i in range(0, producers):
        thread = Thread(target = producer_thread, args = (sqPending, i + 1))
        thread.daemon = True
        thread.start()
    print('\tWaiting for producers to produce all items ...')
    while items_produced < total_items:
        print('\t\tProduced: {0}'.format(items_produced))
        time.sleep(.5)
    print('\t\tProduced {0}'.format(total_items))
    print('\t\tElapsed time: {0}'.format(round(time.time() - producersStart, 3)))
    print('\tWaiting for consumers to consume all items ...')
    while items_consumed < total_items:
        print('\t\tConsumed: {0}'.format(items_consumed))
        time.sleep(.5)
    print('\t\tConsumed {0}'.format(total_items))
    print('\t\tElapsed time: {0}'.format(round(time.time() - consumersStart), 3))
    pendingLen = len(superq('sqPending', attach = True, host = 'local'))
    completedLen = len(superq('sqCompleted', attach = True, host = 'local'))
    print('\tExpected pending superq length = {0}, actual = {1}'.format(0, pendingLen))
    assert(pendingLen == 0)
    print('\tExpected completed superq length = {0}, actual = {1}'.format(total_items, completedLen))
    assert(completedLen == total_items)
    print('\tDeleting superqs ...')
    sqPending.delete()
    sqCompleted.delete()

##    # When uncommented, the following test loops until interrupted
##    print('\nCIRCULAR superq test:\n')
##    sq = superq([1,2,3,2])
##    sqLst = sq._list()
##    sqLst.circular = True
##    for i in sqLst:
##        print('Val: {0}'.format(i.value))
##        time.sleep(1)

    # Note: depending on the OS, this might not free up the bound address immediately
    print('\nShutting down superq network node ...')
    shutdown()

    time.sleep(1)   
except:
    shutdown()
    raise

