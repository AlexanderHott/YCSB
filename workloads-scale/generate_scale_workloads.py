from string import Template

WORKLOAD_TEMPLATE = Template(""" 
recordcount=$OPERATIONS
operationcount=$OPERATIONS
workload=site.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=$READ
updateproportion=$UPDATE
scanproportion=0
insertproportion=$INSERT

requestdistribution=uniform
""")

i_pct = 0.80
u_pct = 0.10
pq_pct = 0.10

baselines = [
    1_000_000,
    5_000_000,
    10_000_000,
    50_000_000,
    100_000_000,
]

for baseline in baselines:
    scale_str = f"{baseline // 1_000_000}".zfill(3)
    filename = f"workload_{scale_str}m"

    workload = WORKLOAD_TEMPLATE.substitute(
        {
            "OPERATIONS": baseline,
            "READ": round(baseline * pq_pct),
            "UPDATE": round(baseline * u_pct),
            "INSERT": round(baseline * i_pct),
        }
    )
    with open(filename, "w") as f:
        f.write(workload)
