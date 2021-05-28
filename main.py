from functools import reduce
#import threading
from multiprocessing import Process, Lock,Manager
import os
import codecs
import time

#Global variables
documents = {}
VARIANT = 22
_THREAD_NUM = 1
_PATHS = ['D:\Python3_Projects/Paralel_course_work/aclImdb/test/neg','D:\Python3_Projects/Paralel_course_work/aclImdb/test/pos',
          'D:\Python3_Projects/Paralel_course_work/aclImdb/train/neg','D:\Python3_Projects/Paralel_course_work/aclImdb/train/pos',
          'D:\Python3_Projects/Paralel_course_work/aclImdb/train/unsup']
_WORD_MIN_LENGTH = 3
_STOP_WORDS = frozenset([
    'a', 'about', 'above', 'above', 'across', 'after', 'afterwards', 'again',
    'against', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although',
    'always', 'am', 'among', 'amongst', 'amoungst', 'amount', 'an', 'and', 'another',
    'any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere', 'are', 'around', 'as',
    'at', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been',
    'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides',
    'between', 'beyond', 'bill', 'both', 'bottom', 'but', 'by', 'call', 'can',
    'cannot', 'cant', 'co', 'con', 'could', 'couldnt', 'cry', 'de', 'describe',
    'detail', 'do', 'done', 'down', 'due', 'during', 'each', 'eg', 'eight',
    'either', 'eleven', 'else', 'elsewhere', 'empty', 'enough', 'etc', 'even',
    'ever', 'every', 'everyone', 'everything', 'everywhere', 'except', 'few',
    'fifteen', 'fify', 'fill', 'find', 'fire', 'first', 'five', 'for', 'former',
    'formerly', 'forty', 'found', 'four', 'from', 'front', 'full', 'further', 'get',
    'give', 'go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her', 'here',
    'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'him',
    'himself', 'his', 'how', 'however', 'hundred', 'ie', 'if', 'in', 'inc',
    'indeed', 'interest', 'into', 'is', 'it', 'its', 'itself', 'keep', 'last',
    'latter', 'latterly', 'least', 'less', 'ltd', 'made', 'many', 'may', 'me',
    'meanwhile', 'might', 'mill', 'mine', 'more', 'moreover', 'most', 'mostly',
    'move', 'much', 'must', 'my', 'myself', 'name', 'namely', 'neither', 'never',
    'nevertheless', 'next', 'nine', 'no', 'nobody', 'none', 'noone', 'nor', 'not',
    'nothing', 'now', 'nowhere', 'of', 'off', 'often', 'on', 'once', 'one', 'only',
    'onto', 'or', 'other', 'others', 'otherwise', 'our', 'ours', 'ourselves', 'out',
    'over', 'own', 'part', 'per', 'perhaps', 'please', 'put', 'rather', 're', 'same',
    'see', 'seem', 'seemed', 'seeming', 'seems', 'serious', 'several', 'she',
    'should', 'show', 'side', 'since', 'sincere', 'six', 'sixty', 'so', 'some',
    'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhere',
    'still', 'such', 'system', 'take', 'ten', 'than', 'that', 'the', 'their',
    'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby',
    'therefore', 'therein', 'thereupon', 'these', 'they', 'thickv', 'thin', 'third',
    'this', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus',
    'to', 'together', 'too', 'top', 'toward', 'towards', 'twelve', 'twenty', 'two',
    'un', 'under', 'until', 'up', 'upon', 'us', 'very', 'via', 'was', 'we', 'well',
    'were', 'what', 'whatever', 'when', 'whence', 'whenever', 'where', 'whereafter',
    'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which',
    'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will',
    'with', 'within', 'without', 'would', 'yet', 'you', 'your', 'yours', 'yourself',
    'yourselves', 'the'])

doc1 = """
    Niners head coach Mike Singletary will let Alex Smith remain his starting 
    quarterback, but his vote of confidence is anything but a long-term mandate.
    Smith now will work on a week-to-week basis, because Singletary has voided 
    his year-long lease on the job.
    "I think from this point on, you have to do what's best for the football team,"
    Singletary said Monday, one day after threatening to bench Smith during a 
    27-24 loss to the visiting Eagles test test test test.
    """

doc2 = """
    The fifth edition of West Coast Green, a conference focusing on "green" home 
    innovations and products, rolled into San Francisco's Fort Mason last week 
    intent, per usual, on making our living spaces more environmentally friendly 
    - one used-tire house at a time.
    To that end, there were presentations on topics such as water efficiency and 
    the burgeoning future of Net Zero-rated buildings that consume no energy and 
    produce no carbon emissions test test.
    """
doc3 = """
    Niners head coach Mike Singletary will let Alex Smith remain his starting 
    quarterback, but his vote of confidence is anything but a long-term mandate.
    Smith now will work on a week-to-week basis, because Singletary has voided 
    his year-long lease on the job.
    "I think from this point on, you have to do what's best for the football team,"
    Singletary said Monday, one day after threatening to bench Smith during a 
    27-24 loss to the visiting Eagles test test test test.
    """
doc4 = """
    The fifth edition of West Coast Green, a conference focusing on "green" home 
    innovations and products, rolled into San Francisco's Fort Mason last week 
    intent, per usual, on making our living spaces more environmentally friendly 
    - one used-tire house at a time.
    To that end, there were presentations on topics such as water efficiency and 
    the burgeoning future of Net Zero-rated buildings that consume no energy and 
    produce no carbon emissions test test.
    """

docs_test = {"doc1":doc1,"doc2":doc2,"doc3":doc3,"doc4":doc4}

def word_split_out(text):
    word_list = []
    wcurrent = []

    for i, c in enumerate(text):
        if c.isalnum():
            wcurrent.append(c)
        elif wcurrent:
            word = u''.join(wcurrent)
            word_list.append(word)
            wcurrent = []

    if wcurrent:
        word = u''.join(wcurrent)
        word_list.append(word)

    return word_list


def word_split(text):
    """
    Split a text in words. Returns a list of tuple that contains
    (word, location) location is the starting byte position of the word.
    """
    word_list = []
    wcurrent = []
    windex = 0

    for i, c in enumerate(text):
        if c.isalnum():
            wcurrent.append(c)
        elif wcurrent:
            word = u''.join(wcurrent)
            word_list.append((windex, word))
            windex += 1
            wcurrent = []

    if wcurrent:
        word = u''.join(wcurrent)
        word_list.append((windex, word))
        windex += 1

    return word_list


def words_cleanup(words):
    """
    Remove words with length less then a minimum and stopwords.
    """
    cleaned_words = []
    for index, word in words:
        if len(word) < _WORD_MIN_LENGTH or word in _STOP_WORDS:
            continue
        cleaned_words.append((index, word))
    return cleaned_words


def words_normalize(words):
    """
    Do a normalization precess on words. In this case is just a tolower(),
    but you can add accents stripping, convert to singular and so on...
    """
    normalized_words = []
    for index, word in words:
        wnormalized = word.lower()
        normalized_words.append((index, wnormalized))
    return normalized_words


def word_index(text):
    """
    Just a helper method to process a text.
    It calls word split, normalize and cleanup.
    """
    words = word_split(text)
    words = words_normalize(words)
    words = words_cleanup(words)
    return words


def inverted_index(text):
    """
    Create an Inverted-Index of the specified text document.
        {word:[locations]}
    """
    inverted = {}

    for index, word in word_index(text):
        locations = inverted.setdefault(word, [])
        locations.append(index)


    return inverted


def inverted_index_add(inverted, doc_id, doc_index,arr_index):
    """
    Add Invertd-Index doc_index of the document doc_id to the 
    Multi-Document Inverted-Index (inverted), 
    using doc_id as document identifier.
        {word:{doc_id:[locations]}}
    """
    mt = Lock()
    for word, locations in doc_index.items():
        mt.acquire()
        indicates = inverted.setdefault(word, {})
        indicates[doc_id] = locations
        inverted[word] = indicates
        mt.release()

    return inverted



def search(inverted, query):
    """
    Returns a set of documents id that contains all the words in your query.
    """
    words = [word for _, word in word_index(query) if word in inverted]
    results = [set(inverted[word].keys()) for word in words]
    return reduce(lambda x, y: x & y, results) if results else []


def distance_between_word(word_index_1, word_index_2, distance):
    """
    To judge whether the distance between the two words is equal distance
    """
    distance_list = []
    for index_1 in word_index_1:
        for index_2 in word_index_2:
            if (index_1 < index_2):
                if (index_2 - index_1 == distance):
                    distance_list.append(index_1)
            else:
                continue
    return distance_list


def extract_text(doc, index):
    """
    Output search results
    """
    word_list = word_split_out(documents[doc])
    word_string = ""
    #for i in range(index):
        #word_string += word_list[i] + " "
    word_string += word_list[index]
    word_string = word_string.replace("\n", "")
    return word_string

def get_data(path_arr):
    """
    read all docs in one dict
    """
    for path in path_arr:
        files = os.listdir(path)
        start_indx = int(len(files)/50 * (VARIANT-1))
        end_indx = int(len(files) / 50 * VARIANT)
        for file in files[start_indx:end_indx]:
        #l = int(len(files)/2)
        #for file in files[0:l]:
            with codecs.open(path+"/"+file,"r","utf-8-sig") as text:
                documents.setdefault(file,text.read())

def show_inverted():
    for word, doc_locations in inverted.items():
        print(word, doc_locations)

def create_inverted(doc_id_arr,text_arr,inverted):
    for i in range(len(doc_id_arr)):
        doc_index = inverted_index(text_arr[i])
        inverted_index_add(inverted, doc_id_arr[i], doc_index,i)


def multi_tread_inverted(docs):
    keys = []
    values = []
    Process_list = []
    for key,val in docs.items():
        keys.append(key)
        values.append(val)

    start = 0
    step = len(keys) / _THREAD_NUM
    end = int(step)

    for num in range(_THREAD_NUM):
        if num == _THREAD_NUM-1:
            end = len(keys)
        x = Process(target=create_inverted,args=(keys[start:end],values[start:end],inverted))
        Process_list.append(x)
        x.start()
        start = end
        end += int(step)

    for th in Process_list:
        th.join()



def search_word_ret_sent(queries):
    for query in queries:
        result_docs = search(inverted, query)
        print("Search for '%s': %r" % (query, result_docs))
        query_word_list = word_index(query)
        for doc in result_docs:
            index_first = []
            distance = 1
            for _, word in query_word_list:
                # if word in _STOP_WORDS:
                #     continue
                index_second = inverted[word][doc]
                if (index_first != []):
                    index_first = distance_between_word(index_first, index_second, distance)
                    distance += 1
                else:
                    index_first = index_second
            for index in index_first:
                print('   - %s...' % extract_text(doc, index)," in ",doc," with word position ",index)

        print("\n")

if __name__ == '__main__':
    manager = Manager()
    inverted = manager.dict()
    get_data(_PATHS)

    while True:
        os.system("cls")
        print("1. Build inverted index")
        print("2. Build inverted index with test data")
        print("3. Search words in inverted index")
        print("4. Change number of threads")
        print("5. Show inverted index")
        print("6. Exit")
        choice = input()
        if str(choice).isnumeric() == False:
            continue
        choice = int(choice)

        if choice == 1:
            os.system("cls")
            inverted.clear()
            start_time = time.time()
            multi_tread_inverted(documents)
            end_time = time.time()
            print("Work time ", end_time - start_time, " with {} threads".format(_THREAD_NUM))
        elif choice == 2:
            os.system("cls")
            inverted.clear()
            start_time = time.time()
            multi_tread_inverted(docs_test)
            end_time = time.time()
            #show_inverted()
            print("Work time ", end_time - start_time, " with {} threads".format(_THREAD_NUM))
        elif choice == 3:
            os.system("cls")
            print("How many words do you wanna search?")
            words_num = int(input())
            queries = []
            os.system("cls")
            for i in range(words_num):
                if i == 0:
                    print("Enter your 1-st word")
                    queries.append(input())
                elif i == 1:
                    print("Enter your 2-nd word")
                    queries.append(input())
                elif i == 2:
                    print("Enter your 3-d word")
                    queries.append(input())
                else:
                    print(f"Enter your {i+1}-th word")
                    queries.append(input())

            search_word_ret_sent(queries)
        elif choice == 4:
            os.system("cls")
            print("Enter number of threads")
            _THREAD_NUM = int(input())
        elif choice == 5:
            os.system("cls")
            show_inverted()
        elif choice == 6:
            break
        else:
            os.system("cls")
            print("Wrong choice try again")
        os.system("pause")
        os.system("cls")




