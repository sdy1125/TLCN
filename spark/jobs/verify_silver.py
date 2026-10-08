"""Read-only consumer-contract verification through Trino HTTP API."""
import argparse
import json
import time
import urllib.request
from silver.registry import CONTRACTS
from silver.contracts import fields, REQUIRED


def query(sql, endpoint='http://trino:8080'):
    request=urllib.request.Request(endpoint+'/v1/statement',data=sql.encode(),headers={'X-Trino-User':'silver-verification'})
    result=[]
    while True:
        with urllib.request.urlopen(request,timeout=120) as response:page=json.load(response)
        if 'error' in page:raise RuntimeError(page['error']['message'])
        result.extend(page.get('data',[]))
        if 'nextUri' not in page:return result
        time.sleep(.1);request=urllib.request.Request(page['nextUri'],headers={'X-Trino-User':'silver-verification'})


def verify():
    result={}
    for target in CONTRACTS:
        table='iceberg.silver.'+target
        columns={row[0] for row in query('SHOW COLUMNS FROM '+table)}
        assert {name.lower() for name in fields(target)}<={name.lower() for name in columns},(target,'missing fields')
        assert not any(c.endswith('_key') for c in columns),(target,'surrogate key')
        missing=' OR '.join(f'{c} IS NULL' for c in REQUIRED)
        count,distinct,invalid=query(f'SELECT count(*),count(DISTINCT source_record_id),count_if({missing}) FROM {table}')[0]
        assert count>0 and count==distinct and invalid==0,(target,count,distinct,invalid)
        partition_count=query(f'SELECT count(*) FROM iceberg.silver."{target}$partitions"')[0][0]
        snapshot=query(f'SELECT snapshot_id FROM iceberg.silver."{target}$snapshots" ORDER BY committed_at DESC LIMIT 1')[0][0]
        result[target]={'rows':count,'unique_keys':distinct,'missing_lineage_or_required':invalid,'partitions':partition_count,'snapshot_id':snapshot}
    sentinel=query('SELECT count_if(temperature_avg_c=-999 OR temperature_max_c=-999 OR temperature_min_c=-999 OR relative_humidity_pct=-999 OR wind_speed_m_s=-999 OR precipitation_mm=-999 OR solar_radiation=-999),count(*) FROM iceberg.silver.weather_daily')[0]
    assert sentinel==[0,614754],sentinel
    result['weather_sentinel_check']=sentinel
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',default='/tmp/silver-trino-verification.json');args=p.parse_args()
    from pathlib import Path
    result=verify();Path(args.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
