# python -m taps.run --config two_nodes.toml

# python -m taps.run --config two_executors.toml

rm -rf /work/cse-zhousc/resilient_compute/resilience_test/taiyi/debug/tmp/generated-files
python -m taps.run --config mapreduce.toml

# python -m taps.run --app fedlearn \
#     --app.dataset mnist --app.data-dir data/fedlearn \
#     --app.rounds 1 --app.participation 0.5 \
#     --engine.executor process-pool

# python -m taps.run --app mapreduce \
#     --app.data-dir data/maildir --app.map-tasks 16 \
#     --engine.executor process-pool

# python -m taps.run --app mapreduce \
#     --app.data-dir /tmp/generated-files --app.map-tasks 16 \
#     --app.generate true --app.generated-files 16 \
#     --engine.executor process-pool

# python -m taps.run --app moldesign \
#     --app.dataset data/moldesign/QM9-search.tsv \
#     --app.initial-count 4 --app.batch-size 4 --app.search-count 16 \
#     --engine.executor process-pool --engine.executor.max-processes 4

# python3 -m taps.run --app docking \
#     --app.smi-file-name-ligand data/docking/dataset_orz_original_1k.csv \
#     --app.receptor data/docking/1iep_receptor.pdbqt \
#     --app.tcl-path data/docking/set_element.tcl
#     --engine.executor process-pool \
#     --engine.executor.max-processes 40 \

# python -m taps.run --app montage --app.img-folder data/Kimages --engine.executor process-pool