#!/bin/sh

cd gromacs/
module load gromacs
grompp -n index.ndx -f grompp.mdp -c input.gro -maxwarn 1
mdrun -nt 1
# /usr/local/gromacs/bin/grompp -n index.ndx -f grompp.mdp -c input.gro -maxwarn 1
# /usr/local/gromacs/bin/mdrun -nt 1

rm -v ener.edr md.log mdout.mdp topol.tpr \#* gromacs-ntl9* confout.gro state* -rf

