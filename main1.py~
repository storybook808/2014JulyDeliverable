from egaugepull import pull_from_egauge
from egaugepull import data_to_csv

data, x, y = pull_from_egauge('2014-07-21 22:15:00','2014-07-21 22:30:00')
outputFile = open('output.csv','wb')
outputFile.write(data)
data_to_csv(data, x, y)


