from flask import Flask, redirect, request, render_template, jsonify
from datetime import datetime
import requests
# http://localhost:5000/{z}/{x}/{y}.png
app = Flask(__name__)

class Tiles:
    def __init__(self):
        self.tiles = {}

    def add_tile(self, x, y, z):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if now in self.tiles.keys():
            self.tiles[now].append((x, y, z))
        else:
            self.tiles[now] = [(x, y, z)]
        # Remove all keys that aren't now
        for key in list(self.tiles.keys()):
            if key != now:
                del self.tiles[key]

    def get_tiles(self):
        return self.tiles


@app.route('/')
def home():
    tiles_list = tiles.get_tiles()
    return render_template('index.html', tiles_list=tiles_list)


@app.route('/tiles', methods=['GET'])
def get_tiles():
    tiles_list = tiles.get_tiles()
    return tiles_list


@app.route('/<int:z>/<int:x>/<int:y>.png')
def proxy(z, x, y):
    global tiles
    # Build the target URL
    mmsi = 219020398
    pstart = '2023-06-01 00:00:00'
    pend = '2023-06-01 04:00:00'


    url = f"http://localhost:7800/public.vector_tile/{z}/{x}/{y}.pbf?mmsi_={mmsi}&p_start={pstart}&p_end={pend}"

    try:
        # Fetch the content from the target URL
        response = requests.get(url, 60)
        if response.status_code == 200:
            return response.content
        else:
            return "Failed to fetch tile", response.status_code
    except Exception as e:
        return f"Error: {e}"

if __name__ == '__main__':
    tiles = Tiles()
    app.run(host='0.0.0.0', port=5000)
