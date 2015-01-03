#! /bin/bash
ant 

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q1.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q1.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ1 -explain without index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q1.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q1.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ1 without index took $DIFF ms \n "

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q6.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q6.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ6 -explain without index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q6.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q6.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ6 without index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q10.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q10.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ10 -explain without index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q10.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q10.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ10 without index took $DIFF ms \n "

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q3.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q3.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ3 -explain without index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q3.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q3.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ3 without index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q19.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q19.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ19 -explain without index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q19.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q19.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ19 without index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q5.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q5.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ5 -explain without index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q5.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q5.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ5 without index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q1.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q1.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nGenerates index for Q1 took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q1.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q1.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ1 -explain with index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q1.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q1.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ1 with index took $DIFF ms \n "

echo -e "java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q6.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q6.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nGenerates index for Q6 took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q6.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q6.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ6 -explain with index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q6.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q6.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ6 with index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q10.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q10.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nGenerates index for Q10 took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q10.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q10.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ10 -explain with index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q10.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q10.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ10 with index took $DIFF ms \n "

echo -e "java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q3.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q3.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nGenerates index for Q3 took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q3.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q3.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ3 -explain with index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q3.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q3.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ3 with index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q19.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q19.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nGenerates index for Q19 took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q19.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q19.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ19 -explain with index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q19.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q19.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ19 with index took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q5.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -index test/TPCH_Q5.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nGenerates index for Q5 took $DIFF ms \n"

echo -e "java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q5.SQL \n"
START=$(date +%s%N)
java -cp build edu.buffalo.cse.sql.Sql -explain test/TPCH_Q5.SQL
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ5 -explain with index took $DIFF ms \n"

START=$(date +%s%N)
echo -e "java -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q5.SQL \n"
java -Xmx1024M -cp build edu.buffalo.cse.sql.Sql test/TPCH_Q5.SQL 
END=$(date +%s%N)
DIFF=$(( $END/1000000 - $START/1000000 ))
echo -e "\nQ5 with index took $DIFF ms \n"



