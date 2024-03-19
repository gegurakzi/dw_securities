import numpy as np
import random

from datetime import datetime
from string import ascii_uppercase, ascii_lowercase, digits

def name_korean(gender=0):
    firstnames = ["김", "이", "박", "최", "정", "강", "조", "윤", "장", "임", "한", "오", "서", "신", "권", "황", "안", "송", "전",
                  "홍", "유", "고", "문", "양", "손", "배", "조", "백", "허", "유", "남", "심", "노", "정", "하"]
    lastnames = ["민준", "서준", "도윤", "예준", "시우", "하준", "지호", "주원", "지후", "준우", "도현", "준서", "건우", "현우", "우진", "지훈",
                 "선우", "유준", "연우", "서진", "은우", "민재", "현준", "시윤", "이준", "정우", "윤우", "승우", "지우", "지환", "승현", "유찬",
                 "준혁", "수호", "승민", "시후", "진우", "민성", "이안", "수현", "준영", "지원", "재윤", "시현", "한결", "태윤", "지안", "윤호",
                 "시원", "동현", "시온", "은찬", "민우", "지한", "재원", "서우", "민규", "은호", "재민", "우주", "민찬", "우빈", "하율", "준호",
                 "율", "지율", "하진", "성민", "승준", "성현", "재현", "현서", "민호", "태민", "준", "지민", "예성", "윤재", "지성", "태현",
                 "로운", "민혁", "하람", "하민", "성준", "규민", "이현", "윤성", "태양", "정민", "은성", "예찬", "도훈", "준수", "준희", "다온",
                 "도하", "주안", "민석", "건", "서연", "서현", "민서", "서윤", "지우", "지민", "윤서", "하은", "은서", "예은", "지윤", "채원",
                 "수빈", "예원", "지원", "하윤", "다은", "서영", "유진", "민지", "가은", "예진", "지유", "예린", "수민", "수아", "소율", "수연",
                 "소윤", "채은", "시은", "연우", "윤아", "현서", "시연", "유나", "예서", "민주", "서진", "유빈", "지아", "다연", "수현", "윤지",
                 "지현", "소연", "지은", "예지", "예나", "혜원", "예빈", "나연", "시현", "지수", "아인", "다인", "연서", "나윤", "다현", "나현",
                 "규리", "하연", "은채", "지연", "주아", "예림", "현지", "민경", "채윤", "민정", "태희", "나경", "하율", "승아", "서율", "도연",
                 "서희", "은지", "수진", "가현", "아영", "하영", "세은", "하린", "서은", "가연", "지율", "보민", "소은", "채린", "민아", "주하",
                 "사랑", "주은", "소현", "정원", "가윤", "지안", "가영", "지효"]

    return firstnames[np.random.randint(len(firstnames))] + lastnames[
        np.random.randint(len(lastnames) // 2 * gender,
                          len(lastnames) // 2 * gender + len(lastnames) // 2)]

def string(length: int, lowercase=None, numbers=False) -> str:
    if lowercase is True:
        letters_set = ascii_lowercase
    elif lowercase is False:
        letters_set = ascii_uppercase
    else:
        letters_set = ascii_lowercase + ascii_uppercase
    if numbers is True:
        letters_set = letters_set + digits
    return ''.join(random.sample(letters_set, length))

def email():
    domains = ["naver.com", "nate.com", "daum.net", "tistory.com", "hanmail.net", "yandex.com",
               "gmail.com", "icloud.com", "cyworld.com", "fastmail.com", "korea.kr", "outlook.com"]
    return string(random.randint(7, 11), lowercase=True, numbers=True) + "@" + choose(domains)

def datetime_normal(avg:datetime):
    return datetime.fromordinal(avg.toordinal() + int(np.random.normal(0.0, 0.25, 1)[0]*28000))

def age_normal(avg:int):
    return abs(int(np.random.normal(avg, 6, 1)[0]))

def income_normal(avg:int):
    return abs(int(np.random.normal(avg, 1000000, 1)[0]))

def binary_probability(probability=1):
    if probability == 1: return 0
    return np.random.randint(probability) // (probability - 1)

def choose(arr: list):
    return arr[random.randrange(len(arr))]

def low_open_high(previous_close):
    return list(map(int, sorted(np.random.normal(previous_close, previous_close*0.015, 3).tolist())))

def price_normal(avg):
    return abs(int(np.random.normal(avg, 10000, 1)[0]))

def sample(arr, size):
    return random.sample(arr, size)