import os
import sys
import time
import random

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

    file_name_to_corrupt = rand.choice(file_names)

    full_path = os.path.join(index_path, file_name_to_corrupt)
    bit_count = os.path.getsize(full_path) * 8
    if bit_count == 0:
        raise RuntimeError(f'oh no, file {full_path} is 0 bytes long, which should never happen in a healthy Lucene index!')
    
    bit_to_flip = rand.randint(0, bit_count - 1)

    if for_real:
        print(f'\n**WARNING**: this tool will soon corrupt bit {bit_to_flip} (of {bit_count} bits) in {full_path}!!!\n\nBe really certain this is what you want... you have 5 seconds to change your mind!\n')
        try:
            for i in range(5):
                print(f'{5-i}...')
                time.sleep(1)
        except KeyboardInterrupt:
            print(f'\nPHEW, DISASTER AVERTED!\n')
            raise
        else:
            print('\n**BOOOOOOOM**\n')

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

            f.seek(int(bit_count / 8))
    else:
        print(f'\nNOTE: would corrupt bit {bit_to_flip} (of {bit_count} bits) in {full_path}; re-run with -real if you really want to do this!')

if __name__ == '__main__':
    main()
