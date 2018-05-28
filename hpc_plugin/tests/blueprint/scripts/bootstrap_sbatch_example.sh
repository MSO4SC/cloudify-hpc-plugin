#!/bin/bash -l

FILE="touch.script"

cat > $FILE <<- EOM
#!/bin/bash -l

#SBATCH -p thin-shared
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --ntasks-per-node=1
#SBATCH -t 00:02:15

# DYNAMIC VARIABLES

sleep 120
touch test_$1.test
EOM
