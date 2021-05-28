import os
import sys
import time
import random

def pick_random_file(rand, index_path, file_names):
    file_name_to_corrupt = rand.choice(file_names)
    full_path = os.path.join(index_path, file_name_to_corrupt)
    size_in_bytes = os.path.getsize(full_path)
    if size_in_bytes == 0:
        raise RuntimeError(f'oh no, file {full_path} is 0 bytes long, which should never happen in a healthy Lucene index!')
    return file_name_to_corrupt, full_path, size_in_bytes

def main():
    for_real = '-real' in sys.argv
    if for_real:
        sys.argv.remove('-real')

    try:
        i = sys.argv.index('-seed')
    except ValueError:
        seed = random.randint(0, sys.maxsize)
    else:
        seed = int(sys.argv[i+1], 16)
        del sys.argv[i:i+2]

    print(f'\nRANDOM SEED: {seed:#x}\n')
    rand = random.Random(seed)

    # poor attempt an ensuring we only use rand below!:
    del globals()['random']

    try:
        random
    except NameError:
        # great!
        pass
    else:
        raise RuntimeError('random still exists!!')

    if len(sys.argv) != 2:
        print(f'Usage: python3 {sys.argv[0]} [-real] [-seed N] /path/to/index')
        sys.exit(1)

    index_path = sys.argv[1]

    file_names = os.listdir(index_path)

    print(f'Directory has {len(file_names)} files:')
    file_names.sort()
    has_dir = False
    for file_name in file_names:
        print(f'  {file_name}')
        if os.path.isdir(os.path.join(index_path, file_name)):
            print('    a directory!?')
            has_dir = True
    if has_dir:
        raise RuntimeError(f'the provided directory ({index_path}) seems to have sub-directories, which a Lucene index should never have!  try again!')

    havoc = rand.randint(0, 3)

    if havoc == 0:
        # flip a single random bit
        file_name_to_corrupt, full_path, size_in_bytes = pick_random_file(rand, index_path, file_names)

        bit_count = size_in_bytes * 8

        bit_to_flip = rand.randint(0, bit_count - 1)
        desc = f'corrupt bit {bit_to_flip} (of {bit_count} bits) in {file_name_to_corrupt}'

        def boom():
            with open(full_path, 'r+b') as f:
                # seek to the right byte
                f.seek(int(bit_to_flip / 8))

                # read current (correct) value
                value = bytearray(f.read(1))

                # find the bit to flip
                bit_offset = bit_to_flip % 8
                mask = 1 << bit_offset

                # the actual bit flip happens here:
                value[0] = value[0] ^ mask

                # seek back again
                f.seek(int(bit_to_flip / 8))

                # write bit-flip'd value
                f.write(value)

    elif havoc == 1:
        # zero out a random 512 byte "disk block"
        page_size = 512
        file_name_to_corrupt, full_path, size_in_bytes = pick_random_file(rand, index_path, file_names)
        
        page_count = int(size_in_bytes / page_size) + 1

        page_to_overwrite = rand.randint(0, page_count - 1)

        desc = f'write page of 512 0 bytese at page={page_to_overwrite} in {file_name_to_corrupt}'
        
        def boom():
            with open(full_path, 'r+b') as f:
                # seek to the right page
                f.seek(page_to_overwrite * page_size)

                # write bit-flip'd value
                f.write(b'\0' * 512)
    elif havoc == 2:
        
        # randomly swap two files!

        # TODO: would be better if we swapped two files in the same segment, or, two files across segments that have the same extension!
        
        file_names = rand.sample(file_names, 2)

        desc = f'swap files {file_names[0]} and {file_names[1]}'

        def boom():
            upto = 0
            while True:
                tmp_file_name = f'__tmp{upto}'
                if not os.path.exists(os.path.join(index_path, tmp_file_name)):
                    break
                upto += 1
            os.rename(os.path.join(index_path, file_names[0]), os.path.join(index_path, tmp_file_name))
            os.rename(os.path.join(index_path, file_names[1]), os.path.join(index_path, file_names[0]))
            os.rename(os.path.join(index_path, tmp_file_name), os.path.join(index_path, file_names[1]))
    elif havoc == 3:
        # randomly truncate a file
        file_name_to_corrupt, full_path, size_in_bytes = pick_random_file(rand, index_path, file_names)

        # 25% of the time, to 0 bytes
        if rand.randint(0, 3) == 2:
            new_length = 0
        else:
            new_length = rand.randint(0, size_in_bytes - 1)

        desc = f'truncate file {file_name_to_corrupt} down to {new_length} bytes from its original {os.path.getsize(full_path)} bytes'

        def boom():
            with open(full_path, 'r+b') as f:
                f.truncate(new_length)
    else:
        raise RuntimeError(f'unknown havoc choice {havoc}!!!')

    if for_real:
        print(f'\n**WARNING**: this tool will soon {desc}!!!\n\nBe really certain this is what you want... you have 5 seconds to change your mind!\n')
        try:
            for i in range(5):
                print(f'{5-i}...')
                time.sleep(1)
        except KeyboardInterrupt:
            print(f'\nPHEW, DISASTER AVERTED!\n')
            raise
        else:
            print('\n**BOOOOOOOM**\n')
            boom()

    else:
        print(f'\nNOTE: would corrupt bit {bit_to_flip} (of {bit_count} bits) in {full_path}; re-run with -real if you really want to do this!')

if __name__ == '__main__':
    main()
