#!/bin/sh

echo "running all examples..."

echo "  running 01_simple..."
cd 01_simple/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 02_join..."
cd ../02_join/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 03_groupby..."
cd ../03_groupby/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 04_sqlalchemy..."
cd ../04_sqlalchemy/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 05_ftp..."
cd ../05_ftp/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 06_subtemplates..."
cd ../06_subtemplates/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 07_filetypes..."
cd ../07_filetypes/
earthmover
rm -rf output/*
echo "  ... done!"

echo "  running 08_html..."
cd ../08_html/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 09_edfi..."
cd ../09_edfi/
earthmover
rm -f output/*
echo "  ... done!"

echo "  running 10_simple..."
cd ../10_simple/
earthmover
rm -rf outputs/*
echo "  ... done!"

cd ../
echo "all examples have run. goodbye :)"