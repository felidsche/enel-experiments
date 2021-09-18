# datagens

----
to generate data for the following experiment workloads the [BDGS fork](https://github.com/felidsche/BigDataBench_V5.0_BigData_ComponentBenchmark/tree/main/BigDataGeneratorSuite) is used
## Analytics
```bash
Table_datagen/e-com/generate_table.sh
# input 1
```
## LDA
- *Note*: this requires GSL, on MacOS it can be installed following [this Guide](https://coral.ise.lehigh.edu/jild13/2016/07/11/hello/)
- for a sample, see: `samples/LDA.txt` (created using `./gen_text_data.sh wiki_noSW_90_Sampling 1 100 10 gen_data/`)
- `stopwords.txt` is from [add repo]()
```bash
# go into the cloned repo
cd ~/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Text_datagen
# run the data generation for the model wiki_noSW_90_Sampling  <NO_FILES> <NO_LINES> <expectation words of each file> <OUTPUT_PATH>
./gen_text_data.sh wiki_noSW_90_Sampling 1 100 10000 gen_data/
```