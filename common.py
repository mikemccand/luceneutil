import sys
import os

if sys.platform.lower().find('darwin') != -1:
  osName = 'osx'
elif sys.platform.lower().find('cygwin') != -1:
  osName = 'cygwin'
elif sys.platform.lower().find('win') != -1:
  osName = 'windows'
elif sys.platform.lower().find('linux') != -1:
  osName = 'linux'
else:
  osName = 'unix'

def pathsep():
  if osName == 'cygwin':
    return ';'
  else:
    return os.pathsep
