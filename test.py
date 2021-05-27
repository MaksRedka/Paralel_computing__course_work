import threading
import queue
import os

files = os.listdir('D:/Python3_Projects/Paralel_course_work/aclImdb/test/neg')
print(len(files))
print(files[5250:5500])
ar1 = ["I just attended a preview scre","asdasdasdasd"]
el = ["a","b"]
with open('D:\Python3_Projects\Paralel_course_work/aclImdb/test/neg/3476_1.txt') as text:
    #print(text.read())
    a = text.read()
    print(a)
test = dict.fromkeys(files)
test.update({files[0]:f"{a}"})

print(test)

arr = []

cnt = 0
que = queue.Queue()

def increment():
    for i in range(5):
        arr.append(i)
        #print(cnt)
    #print(cnt)

#target=lambda q,args1: q.put(increment(args1)),args=(que,cnt,)
th = threading.Thread(target=increment,args=())
th.start()


increment()
th.join()
#cnt += que.get()

print("====",arr,"===")
