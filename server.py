#!/usr/bin/env python3
"""Local server for the SEDA Composite Feed Explorer.

Serves the tool and exposes /api/refresh to re-pull all venue data.
  python3 server.py            -> http://localhost:8787
  POST /api/refresh            -> full refresh incl. depth (slow)
  POST /api/refresh?quick=1    -> volume+price only (fast), reuse cached depth
"""
import http.server, socketserver, json, os, subprocess, sys, urllib.parse

HERE=os.path.dirname(os.path.abspath(__file__))
PORT=int(os.environ.get("PORT","8787"))

class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self,*a,**k): super().__init__(*a,directory=HERE,**k)
    def log_message(self,*a): pass
    def do_POST(self):
        if self.path.split("?")[0]=="/api/refresh":
            qs=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            quick="--quick" if qs.get("quick") else None
            cmd=[sys.executable,os.path.join(HERE,"refresh_data.py")]+([quick] if quick else [])
            try:
                subprocess.run(cmd,check=True,cwd=HERE,timeout=1800)
                data=open(os.path.join(HERE,"data.json"),"rb").read()
                self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
            except Exception as e:
                msg=json.dumps({"error":str(e)}).encode()
                self.send_response(500); self.send_header("Content-Type","application/json"); self.end_headers(); self.wfile.write(msg)
        else:
            self.send_response(404); self.end_headers()
    def end_headers(self):
        self.send_header("Cache-Control","no-store"); super().end_headers()

if __name__=="__main__":
    socketserver.TCPServer.allow_reuse_address=True
    with socketserver.TCPServer(("127.0.0.1",PORT),H) as httpd:
        print(f"SEDA Composite Feed Explorer → http://localhost:{PORT}")
        httpd.serve_forever()
