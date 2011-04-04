from competition import Competition
from competition import WIKI_MEDIUM

if __name__ == '__main__':
  comp =  Competition(ramBufferMB=1024, printCharts=True)
  comp.competitor('trunk').withIndex('trunk', WIKI_MEDIUM)
  comp.competitor('modified_trunk').withIndex('modified_trunk', WIKI_MEDIUM)
  # indexOnly
  # comp.skipSearch()
  
  # search only -- if index is prebuild
  # comp.skipIndex()
  
  comp.benchmark("trunk vs. modified_trunk")
  
