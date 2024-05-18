- qViz class to interact with the temporal controller.
- mobDB singleton to connect to the mobilitydb database


On new temporal controller tick t => load datapoints for the range [t-BUFFER, t+ BUFFER], with BUFFER the amount of frames needed for 30 FPS animation.

1. Creating all the qgis features during the initial loading
2. Creating qgis features on the fly