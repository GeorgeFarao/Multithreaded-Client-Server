#!/bin/bash

if [ $# -ne 5 ]; then
	echo "You need to give 5 command line arguments"
	exit 2
fi

mkdir -p -- $3

numFilesPerDirectory=$4
numRecordsPerFile=$5

countriesFile=$2

count=0
#for line in 'cat $countriesFile'
while IFS= read -r line 
do
	mkdir -p -- "$3/$line"
	((count++))
done < "$countriesFile"


random-string(){
	RANGE=12
	FLOOR=3

	number=0

	while [ "$number" -le $FLOOR ]
	do
		number=$RANDOM
		let "number %= $RANGE"
	done
	cat /dev/urandom | tr -dc 'a-zA-Z' | fold -w ${1:-$number} | head -n 1
}

temp=0



max_recs=$(expr "$numRecordsPerFile" '*' "$numFilesPerDirectory" )
#echo "$max_recs"
let breakpoint=$max_recs/6
#echo "$breakpoint"







for x in "$3"/*/; do
	arr_it=0
	declare -a rec_array
	for (( k=0; k<($max_recs-$breakpoint); k++))
	do
		
		disease=$(shuf -n 1 $1) 
		disease="${disease// /}"

		ageRANGE=120
		ageFLOOR=1

		age=0

		while [ "$age" -le $ageFLOOR ]
		do
			age=$RANDOM
			let "age %= $ageRANGE"
		done

		first_name=$(random-string)
		last_name=$(random-string)

		stRANGE=200
		stFLOOR=1
		coin=$RANDOM
		let "coin %= $stRANGE"
	    status="x"
		one="1"
		if [ "$coin" -le 190 ]
		then
			status="ENTER"
		else
			status="EXIT"
		fi	

		rec_array[$k]="$temp $status $(random-string) $(random-string) $disease $age"
		((temp++))
		echo "$temp"
	done
	
	for (( l =($max_recs-$breakpoint) ; l < ($max_recs); l++ ))
	do
		r=$RANDOM
		let "r %= ($max_recs-$breakpoint)"
		#echo "$r"
		str=${rec_array[$r]}
		#echo "$str"
		IFS=' '
		read -ra word <<< "$str"
		#echo ${word[1]}
		if [ "${word[1]}" = "ENTER" ]
		then
			#echo "ok"
			word[1]="EXIT"
		fi
		#echo ${word[@]}
		#echo "\\"
		rec_array[$l]="${word[@]}"

	done			





	for(( i=0; i<$numFilesPerDirectory; i++))
	do 
		RANGE1=30
		FLOOR1=1

		number1=0

		while [ "$number1" -le $FLOOR1 ]
		do
			number1=$RANDOM
			let "number1 %= $RANGE1"
		done

		if [ "$number1" -lt "10" ]
		then 
			#let "number1 = "0"$number1"
			number1="0$number1"
		fi	

		RANGE2=12
		FLOOR2=1

		number2=0

		while [ "$number2" -le $FLOOR2 ]
		do
			number2=$RANDOM
			let "number2 %= $RANGE2"
		done

		if [ "$number2" -lt "10" ]
		then 
			#let "number2 = "0"$number1"
			number2="0$number2"
		fi	

		RANGE3=2020
		FLOOR3=1950

		number3=0

		while [ "$number3" -le $FLOOR3 ]
		do
			number3=$RANDOM
			let "number3 %= $RANGE3"
		done

		touch -- "$x"/"$number1-$number2-$number3.txt"
		for(( j=0; j<$numRecordsPerFile; j++))
		do 

			echo "${rec_array[$arr_it]}" >> "$x"/"$number1-$number2-$number3.txt"
			((arr_it++))
		done	
	done
	unset rec_array
done
