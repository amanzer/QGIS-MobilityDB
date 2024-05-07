from flask import Flask, redirect, request, render_template
from datetime import datetime

# URL of QGIS map : http://localhost:5000/{z}/{x}/{y}.png

app = Flask(__name__)

class Tiles :
    def __init__(self):
        self.tiles= {}


    def add_tile(self, x, y, z):
        now = datetime.now()
        #string of now
        now = now.strftime("%Y-%m-%d %H:%M:%S")
        
        #check if now in self.tiles keys

        if now in self.tiles.keys():
            #if now in keys, add the tile to the list
            self.tiles[now].append((x, y, z))
        else:
            self.tiles[now] = [(x,y,z)]

        #remove all keys that aren't now
        for key in self.tiles.keys():
            if key != now:
                del self.tiles[key]
        
    def get_tiles(self):
        return self.tiles


@app.route('/')
def home():
    tiles_list = tiles.get_tiles()
    return render_template('index.html', tiles_list=tiles_list)


@app.route('/<int:z>/<int:x>/<int:y>.png')
def proxy(z, x, y):
    global tiles
    # Build the target URL
    
    target_url = f'https://tile.openstreetmap.org/{z}/{x}/{y}.png'
    #print(f'Requesting x : {x} y : {y} z : {z}')
    # Redirect the request to the target URL
    tiles.add_tile(x, y, z)
    return redirect(target_url)



if __name__ == '__main__':
    tiles = Tiles()
    app.run(host='0.0.0.0', port=5000)
