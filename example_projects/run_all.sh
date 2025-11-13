#!/bin/sh

echo "running all examples..."

echo "  running 01_simple..."
cd 01_simple/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 02_join..."
cd ../02_join/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 03_groupby..."
cd ../03_groupby/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 03a_groupby_with_rank..."
cd ../03a_groupby_with_rank/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 04_sqlalchemy..."
cd ../04_sqlalchemy/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 05_ftp..."
cd ../05_ftp/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 06_subtemplates..."
cd ../06_subtemplates/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 07_filetypes..."
cd ../07_filetypes/
earthmover run -f
rm -rf output/*
echo "  ... done!"

echo "  running 08_html..."
cd ../08_html/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 09_edfi..."
cd ../09_edfi/
earthmover run -f
rm -f output/*
echo "  ... done!"

echo "  running 10_jinja..."
cd ../10_jinja/
earthmover run -f
rm -rf outputs/*
echo "  ... done!"

echo "  running 11_composition..."
cd ../11_composition/
earthmover deps
earthmover run -f
rm -rf output/*, packages/*
echo "  ... done!"

echo "  running 12_melt_pivot..."
cd ../12_melt_pivot/
earthmover run -f
rm -rf output/*
echo "  ... done!"

cd ../
echo "all examples have run, goodbye"