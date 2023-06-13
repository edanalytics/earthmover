#!/bin/sh

echo "running all examples..."

echo "  running 01_simple..."
cd 01_simple/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 02_join..."
cd ../02_join/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 03_groupby..."
cd ../03_groupby/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 04_sqlalchemy..."
cd ../04_sqlalchemy/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 05_ftp..."
cd ../05_ftp/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 06_subtemplates..."
cd ../06_subtemplates/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 07_filetypes..."
cd ../07_filetypes/
earthmover -f
rm -rf output/*
echo "  ... done!"

echo "  running 08_html..."
cd ../08_html/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 09_edfi..."
cd ../09_edfi/
earthmover -f
rm -f output/*
echo "  ... done!"

echo "  running 10_jinja..."
cd ../10_jinja/
earthmover -f
rm -rf outputs/*
echo "  ... done!"

cd ../
echo "all examples have run. goodbye :)"