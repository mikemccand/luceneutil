import base64
import cgi
import subprocess
import wsgiref.simple_server
import socket

import sys
sys.path.insert(0, '/home/changingbits/webapps/examples/htdocs')
import localconstants

def application(environ, startResponse):
  _l = []
  w = _l.append

  args = cgi.parse(environ=environ)
  #print('GOT ARGS: %s' % str(args))
  if 'terms' in args:
    terms = args['terms'][0]
  else:
    terms = None

  w('<html>')
  w('<body>')
  w('<h2>Build your own FST</h2>')
  if terms is None:
    terms = '''mop/0
moth/1
pop/2
star/3
stop/4
top/5
'''

  w('<table>')
  w('<tr>')
  w('<td valign=top>')
  w('<form method=GET action="/fst">')
  w('<textarea name="terms" cols=50 rows=10>')
  if terms is not None:
    w(terms)
  w('</textarea>')
  w('<br>')
  w('<input type=submit name=cmd value="Build it!">')
  w('</form>')
  w('</td>')
  w('<td valign=top>')
  w('<ul>')
  w('<li> Each entry can be an input (creates an FSA) or input/ouput (creates an FST).')
  w('<li> Separate each entry with space or newline.')
  w('<li> If all outputs are ints >= 0 then outputs are numeric (sum as you traverse); otherwise outputs are strings (concatenate as you traverse).')
  w('<li> NEXT-optimized arcs (whose target is the next node) are <font color=red>red</font>.')
  w('<li> A bolded arc means the next node is final.')
  w('<li> See <a href="http://blog.mikemccandless.com/2013/06/build-your-own-finite-state-transducer.html">this blog post</a> for details and examples.')
  w('</ul>')
  w('</tr>')
  w('</table>')

  if terms is not None:

    l = terms.split()
    error = None
    for x in l:
      tup = x.split('/')
      if len(tup) > 2:
        error = 'Each item should be input or input/output string; got invalid item: %s' % x
        break
      
    if error is not None:
      w('<font color=red>%s</font>' % error)
      
    p = subprocess.Popen(['java', '-cp', '%s:%s' % (localconstants.BUILD_FST_PATH, localconstants.LUCENE_JAR), 'BuildFST'] + l, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    dotString, err = p.communicate()
    if p.returncode != 0:
      w('<br>')
      w('Sorry, BuildFST failed:<br>')
      w('<pre>%s</pre>' % err.decode('utf-8'))
    else:
      #print('got dot: %s' % dotString.decode('ascii'))
      p = subprocess.Popen(['dot', '-Tpng'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      pngData, err = p.communicate(dotString)
      if p.returncode != 0:
        w('<br>')
        w('Sorry, dot failed:<br>')
        w('<pre>%s</pre>' % err.decode('utf-8'))
      else:
        w('<br><br>')
        w('<img src="data:image/png;base64,%s">' % base64.b64encode(pngData).decode('ascii'))
  w('</body>')
  w('</html>')

  html = ''.join(_l)

  headers = []
  headers.append(('Content-Type', 'text/html'))
  headers.append(('Content-Length', str(len(html))))

  startResponse('200 OK', headers)
  return [html.encode('utf-8')]

def main():
  port = 11000
  httpd = wsgiref.simple_server.make_server('0.0.0.0', port, application)
  print('Ready on port %s' % port)
  httpd.serve_forever()

if __name__ == '__main__':
  main()
  
  
  
