import os
import socket
import ssl

MAXLEN = 32

def send(sock, msg):
    buffer = bytes(msg, 'utf-8')
    
    totalSent = 0
    while totalSent < len(buffer):
        bytesSent = sock.send(buffer[totalSent:])
        if bytesSent == 0:
            raise RuntimeError('Connection closed.')
        totalSent = totalSent + bytesSent

def recv(sock):
    msg = ''

    while len(msg) < 1:
        buffer = sock.recv(MAXLEN - len(msg))
        if len(buffer) == 0:
            raise RuntimeError('Connection closed.')
        msg = msg + buffer.decode('utf-8')
        
    return msg

# connect to server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# convert socket to ssl
ssl_sock = ssl.wrap_socket(s,
                           cert_reqs = ssl.CERT_NONE,
                           ssl_version = ssl.PROTOCOL_TLSv1)

ssl_sock.connect(('127.0.0.1', 9999))

# write test
send(ssl_sock, 'hello world!')

# read test
print(recv(ssl_sock))

# cleanup
ssl_sock.close()
