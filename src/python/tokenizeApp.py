import base64
import cgi
import subprocess
import sys
import wsgiref.simple_server

sys.path.insert(0, "/home/changingbits/webapps/examples/htdocs")
import localconstants


def application(environ, startResponse):
  _l = []
  w = _l.append

  args = cgi.parse(environ=environ)
  # print('GOT ARGS: %s' % str(args))
  if "text" in args:
    text = args["text"][0]
  else:
    text = None

  w("<html>")
  w("<body>")
  w("<h2>Analyzer</h2>")
  if text is None:
    text = "the dog runs quickly and barks"

  w("<table>")
  w("<tr>")
  w("<td valign=top>")
  w('<form method=GET action="/tokenize.py">')
  w('<textarea name="terms" cols=50 rows=10>')
  if text is not None:
    w(text)
  w("</textarea>")
  w("<br>")
  w('<input type=submit name=cmd value="Tokenize!">')
  w("</form>")
  w("</td>")
  w("<td valign=top>")
  w("<ul>")
  w("</ul>")
  w("</tr>")
  w("</table>")

  if text is not None:
    l = terms.split()  # noqa: F821 # pyright: ignore[reportUndefinedVariable] # TODO: where is terms??? does this file work?

    p = subprocess.Popen(
      ["java", "-cp", "%s:%s" % (localconstants.STAGE_TOKENIZER_PATH, localconstants.LUCENE_JAR), "StageTokenizer"] + l, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    result, err = p.communicate()
    if p.returncode != 0:
      w("<br>")
      w("Sorry, StageTokenizer failed:<br>")
      w("<pre>%s</pre>" % err.decode("utf-8"))
    else:
      # print('got dot: %s' % dotString.decode('ascii'))
      p = subprocess.Popen(["dot", "-Tpng"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      pngData, err = p.communicate(result)
      if p.returncode != 0:
        w("<br>")
        w("Sorry, dot failed:<br>")
        w("<pre>%s</pre>" % err.decode("utf-8"))
      else:
        w("<br><br>")
        w('<img src="data:image/png;base64,%s">' % base64.b64encode(pngData).decode("ascii"))
  w("</body>")
  w("</html>")

  html = "".join(_l)

  headers = []
  headers.append(("Content-Type", "text/html"))
  headers.append(("Content-Length", str(len(html))))

  startResponse("200 OK", headers)
  return [html.encode("utf-8")]


def main():
  port = 11000
  httpd = wsgiref.simple_server.make_server("0.0.0.0", port, application)
  print("Ready on port %s" % port)
  httpd.serve_forever()


if __name__ == "__main__":
  main()
