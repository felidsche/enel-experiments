# datagens

----
to generate data for the following experiment workloads the [BDGS fork](https://github.com/felidsche/BigDataBench_V5.0_BigData_ComponentBenchmark) is used
- ```bash
  cd ~/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite
  ```
## Analytics
```bash
cd Table_datagen/e-com
./generate_table.sh
# input 1
```
- eComHDFSWriter is not used, see `BigDataGeneratorSuite/Table_datagen/e-com-generator/README.md`
## LDA
- *Note*: this requires GSL, on MacOS it can be installed following [this Guide](https://coral.ise.lehigh.edu/jild13/2016/07/11/hello/)
- for a sample, see: `samples/LDA.txt` (created using `./gen_text_data.sh wiki_noSW_90_Sampling 1 100 10 gen_data/`)
- `stopwords.txt` is from [add repo]()
```bash
cd Text_datagen
# run the data generation for the model wiki_noSW_90_Sampling  <NO_FILES> <NO_LINES> <expectation words of each file> <OUTPUT_PATH>
./gen_text_data.sh wiki_noSW_90_Sampling 1 100 10000 gen_data/
```

## GBT
- creates matrix data in `.csv` format: `label, feature1, feature2, featureN`
- Usage: `SGDDataGenerator` on HDFS 
``` bash
SGDDataGenerator <samples> <dimension> <output> <defaultHdfsFs>
```
- Usage: `SGDDataGeneratorLocal` (creates) `samples/sgd.txt` 
```bash
SGDDataGeneratorLocal <samples> <dimension> <output>
```

## PageRank
- **Note**: this is only tested on Ubuntu 20.04, it doesnt work on MacOS Catalina
- uses the Google graph
- Parameters:
- `-o`: Output graph file name (default:'graph.txt')
- `-m`: Matrix (in Maltab notation) (default:'0.9 0.5; 0.5 0.1')
- `-i`: Iterations of Kronecker product (default:5)
- `-s`: Random seed (0 - time seed) (default:0)
```bash
cd Graph_datagen
./gen_kronecker_graph -o:google_g_16.txt -m:"0.8305 0.5573; 0.4638 0.3021"
```