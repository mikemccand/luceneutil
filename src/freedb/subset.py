import random

f = open('all.txt', 'rb')
while True:
  line = f.readline()
  if line == '':
    break
  for song in line.strip().split('\t')[1:]:
    if random.random() < .0001:
      print song.strip()
f.close()

