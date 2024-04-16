import http.server
import socketserver

PORT = 8000
LOG_FILE = 'C:/MY/ELAIT/SNOWFLAKE/POC/MIGRATION/LOG_DATA/migration.log'

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
