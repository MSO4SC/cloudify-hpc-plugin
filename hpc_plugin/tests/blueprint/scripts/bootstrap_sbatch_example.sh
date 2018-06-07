#!/bin/bash -l

FILE="touch.script"

cat > $FILE <<- EOM
#!/bin/bash -l

#SBATCH -p $2
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --ntasks-per-node=1
#SBATCH -t 00:15:00

# DYNAMIC VARIABLES

sleep 900
touch test_$1.test
EOM
