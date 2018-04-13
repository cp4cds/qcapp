#!/bin/bash
# /group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1
# BNU/BNU-ESM/piControl/fx/atmos/fx/r0i0p0/orog/files/20130507/orog_fx_BNU-ESM_piControl_r0i0p0.nc


find /group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1/ \
    -mindepth 4 -maxdepth 4 \
    \( -name fx  \) | while read fxbasedir

do
   find -L $fxbasedir -mindepth 7 -maxdepth 7 -type f -name '*_fx_*.nc'
done
