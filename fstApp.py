import base64
import cgi
import subprocess
import wsgiref.simple_server

LUCENE_JAR = '/l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar'

# bad: http://10.17.4.91:11000/fst?terms=mop%2F1%0D%0Amoth%0D%0Apop%2F4%0D%0Astar%0D%0Astop%0D%0Atop%2F3%0D%0A%0D%0A&cmd=Build+FST

def application(environ, startResponse):
  _l = []
  w = _l.append

  if environ['PATH_INFO'] != '/fst':
    startResponse('404 Not Found', [])
    return []

  #print('environ: %s' % str(environ))

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
    terms = '''mop/1
moth/2
pop/3
star/4
stop/5
top/6
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
  w('<li> Each entry can be input (creates an FSA) or input/ouput (creates an FST).')
  w('<li> Separate each entry with space or newline.')
  w('<li> If all outputs are ints > 0 then outputs are numeric (sum as you traverse); otherwise outputs are strings (concatenate as you traverse).')
  w('<li> NEXT-optimized arcs (whose target is the next node) are <font color=red>red</font>.')
  w('<li> A bolded arc means the next node is final.')
  w('<li> See <a href="http://blog.mikemccandless.com">this blog post</a> for details and examples.')
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
      
    p = subprocess.Popen(['java', '-cp', '.:%s' % LUCENE_JAR, 'BuildFST'] + l, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
  
  
  
