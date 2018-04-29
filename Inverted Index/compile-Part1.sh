rm -r class-Part1/*
javac -classpath hadoop-core-1.0.3.jar -d class-Part1 code-Part1/*
#javac -classpath hadoop-core-1.0.3.jar -d class part2/*
jar -cvf HW2-Part1.jar -C class-Part1/ .
