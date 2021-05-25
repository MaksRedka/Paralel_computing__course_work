import threading
import queue
import os
arr = os.listdir('D:\Python3_Projects\Paralel_course_work')
print(arr)


cnt = 0
que = queue.Queue()

def increment(cnt):
    for i in range(5):
        cnt +=1
        print(cnt)
    print(cnt)
    return cnt

th = threading.Thread(target=lambda q,args1: q.put(increment(args1)),args=(que,cnt,))
th.start()


cnt += increment(cnt)
th.join()
cnt += que.get()

print("====",cnt,"===")
