/!\ Pymeos now works within QGIS python environment without issue thanks to Maxime Schoemans. 

Pymeos used to crash QGIS when called directly within its python environment due to name resolution conflict.
In this folder, you can find code that uses subprocess to execute pymeos code in an external process.