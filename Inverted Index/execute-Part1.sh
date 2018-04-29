hadoop dfs -rmr HW3/inverted_table/
#hadoop dfs -rmr caOutput
hadoop jar HW2-Part1.jar part1.WordCount hdfs://Quanta006:8100/opt/Assignment3/Input/100M/ HW3/inverted_table/
#hadoop jar Lab1.jar part2.CalculateAverage caInput caOutput 
#hadoop dfs -cat Output_Inverted/*
#hadoop dfs -cat caOutput/*
