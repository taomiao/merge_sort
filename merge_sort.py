# -*- coding: utf-8 -*-
# @Time    : 2020/9/24 11:08
# @Author  : Miao Tao
# @Email   : miao_tao@shannonai.com
# @File    : merge_sort.py

from multiprocessing import Pool
import random
import string
import sys
from typing import List
from queue import PriorityQueue
from ctypes import c_uint64

int_size = 8
total_data_size = 4*1024
per_process_size = 1024
per_data_size = int_size+32

class KV(object):
    def __init__(self, k, v):
        self.key = k
        self.val = v

    def __lt__(self, other):
        if self.key < other.key or (self.key == other.key and self.val < other.val):
            return True
        elif self.key == other.key and self.val == other.val:
            return False
        else:
            return False

    def __str__(self):
        return str(self.key) + " : " + str(self.val)

    def __repr__(self):
        return str(self.key) + " : " + str(self.val)

def create_test_file(file_name: str):
    with open(file_name, "wb") as test_file:
        r = random.Random()
        for i in range(total_data_size):
            key = c_uint64(r.randint(0, 100000))
            val = ''.join(random.sample(string.ascii_letters + string.digits, 32))
            print(key, val)
            test_file.write(key)
            test_file.write(val.encode("ascii"))

def parse_one(d):
    key = int.from_bytes(d[:int_size], sys.byteorder)
    val = d[int_size:per_data_size]
    return KV(key, val)

def sort_part(file_name: str, start_pos: int, part_size: int):
    with open(file_name, "rb") as in_file:
        in_file.seek(start_pos)
        d = in_file.read(part_size)
        data = set()
        for i in range(int(part_size/per_data_size)):
            kv = parse_one(d[i*per_data_size:i*per_data_size+per_data_size])
            data.add(kv)
        data = sorted(data)
        print(data)
        with open(file_name+"_"+str(start_pos), "wb") as o_file:
            for item in data:
                o_file.write(c_uint64(item.key))
                o_file.write(item.val)
        return 0

def merge_parts(input_files: List[str], output_file: str):
    q = PriorityQueue()
    with open(output_file, "wb") as output:
        in_files = []
        for f in input_files:
            in_files.append(open(f, "rb"))
        for idx, f in enumerate(in_files):
            tmp = f.read(per_data_size)
            tmp = parse_one(tmp)
            q.put((tmp, idx, f))
        # todo:读文件优化，IO效率低下，加buffer
        while not q.empty():
            (d, idx, f) = q.get()
            output.write(c_uint64(d.key))
            output.write(d.val)
            print(d.key, d.val)
            tmp = f.read(per_data_size)
            if tmp:
                tmp = parse_one(tmp)
                q.put((tmp, idx, f))

        for f in in_files:
            f.close()

def sort_for_file(input_file: str, output_file: str):
    handle_ps = Pool(processes=1)
    tasks = []
    for i in range(int(total_data_size/per_process_size)):
        start_pos = i * per_process_size * per_data_size
        part_size = per_process_size * per_data_size
        tasks.append((i, start_pos, part_size))
    for t in tasks:
        handle_ps.apply_async(sort_part, (input_file, t[1], t[2]))
    handle_ps.close()
    handle_ps.join()
    part_files = []
    for t in tasks:
        part_files.append(input_file+"_"+str(t[1]))
    merge_parts(part_files, "out.bin")


if __name__ == "__main__":
    create_test_file("test.bin")
    #sort_part("test.bin",0, per_data_size*4)
    #sort_part("test.bin", per_data_size*2, per_data_size * 2)
    #merge_parts(["test.bin_0", "test.bin_80"], "out.bin")
    sort_for_file("test.bin", "out.bin")