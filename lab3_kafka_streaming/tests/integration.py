"""Real-broker assertions, recorded with topic IDs and committed offsets."""
import contextlib
import csv
import importlib.util
import io
import json
import os
import sys
import time
import uuid
from pathlib import Path
from itertools import combinations
from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic, NewPartitions
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import consumer_args, make_producer, json_bytes

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'evidence';OUT.mkdir(exist_ok=True)
BOOT=os.environ.get('KAFKA_BOOTSTRAP','localhost:9092')
PREFIX='verify_'+uuid.uuid4().hex[:8]
results={'run_id':PREFIX,'bootstrap':BOOT,'broker_mode':os.environ.get('BROKER_MODE','native Kafka 3.5.2'), 'checks':{}}


def load(path):
    spec=importlib.util.spec_from_file_location(Path(path).stem,ROOT/path)
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod


def call(name,fn,*args,**kwargs):
    with (OUT/(name+'.jsonl')).open('w') as target,contextlib.redirect_stdout(target):
        value=fn(*args,**kwargs)
        print(json.dumps({'summary':value}))
    results['checks'][name]=value
    print(name,value,flush=True)
    return value


def read_all(topic):
    c=KafkaConsumer(bootstrap_servers=BOOT,enable_auto_commit=False)
    partitions=[TopicPartition(topic,p) for p in sorted(c.partitions_for_topic(topic))]
    c.assign(partitions);c.seek_to_beginning(*partitions)
    ends=c.end_offsets(partitions);rows=[];deadline=time.monotonic()+30
    try:
        while any(c.position(p)<ends[p] for p in partitions):
            if time.monotonic()>deadline:raise AssertionError('read deadline')
            rows.extend(r for batch in c.poll(timeout_ms=500).values() for r in batch)
    finally:c.close(autocommit=False)
    return rows


def assignments(topic,n):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Lock
    group=PREFIX+'_members'+str(n)
    stop=Event();lock=Lock();states={}
    def worker(i):
        c=KafkaConsumer(topic,**consumer_args(BOOT,group,'member-'+str(i)))
        try:
            while not stop.is_set():
                c.poll(timeout_ms=250)
                generation=c._coordinator.generation()
                with lock:
                    states[i]=(set(c.assignment()),generation is not None)
        finally:c.close(autocommit=False)
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures=[pool.submit(worker,i) for i in range(n)]
        deadline=time.monotonic()+45
        value=None
        try:
            while time.monotonic()<deadline:
                for future in futures:
                    if future.done():future.result()
                with lock: snapshot=dict(states)
                if len(snapshot)==n and all(v[1] for v in snapshot.values()):
                    assigned=[snapshot[i][0] for i in range(n)]
                    if (len(set().union(*assigned))==3 and
                        all(a.isdisjoint(b) for a,b in combinations(assigned,2)) and
                        sum(bool(a) for a in assigned)==min(n,3)):
                        value=[[p.partition for p in sorted(a,key=lambda p:p.partition)] for a in assigned]
                        break
                time.sleep(.2)
            if value is None:raise AssertionError('stable assignments not reached')
        finally:stop.set()
        for future in futures:future.result()
    return value


csvp=load('csv_lab/producer_csv.py');csvc=load('csv_lab/consumer_csv.py')
jsonp=load('json_lab/producer_json.py');jsonc=load('json_lab/consumer_json.py')
topics={n:PREFIX+'_'+n for n in ['csv','dirty_csv','raw','filtered','rejected','dirty_raw','dirty_rejected','schema','schema_ok','schema_bad']}
admin=KafkaAdminClient(bootstrap_servers=BOOT)
admin.create_topics([NewTopic(t,3,1) for t in topics.values()])
try:
    clean=ROOT/'csv_lab/data/transactions.csv';dirty=ROOT/'csv_lab/data/transactions_dirty.csv'
    assert call('csv_produce',csvp.produce,clean,BOOT,topics['csv'],0)==200
    csv_records=read_all(topics['csv'])
    assert len(csv_records)==200
    assert len({(r.partition,r.offset) for r in csv_records})==200
    # Same user key stays in one partition before changing partition count.
    by_key={}
    for r in csv_records:by_key.setdefault(r.key,set()).add(r.partition)
    assert all(len(p)==1 for p in by_key.values())
    per_user={};per_partition={}
    for r in csv_records:
        tid=int(next(csv.reader([r.value.decode()]))[0])
        per_user.setdefault(r.key,[]).append(tid)
        per_partition.setdefault(r.partition,[]).append(r.offset)
    assert all(ids==sorted(ids) for ids in per_user.values())
    assert all(offsets==sorted(offsets) for offsets in per_partition.values())
    results['checks']['ordering']={'per_user_in_order':True,'per_partition_offsets_in_order':True,
                                  'partition_count':len(per_partition)}
    group=PREFIX+'_resume'
    assert call('csv_first17',csvc.consume,BOOT,topics['csv'],group,17,5)['processed']==17
    assert call('csv_resume183',csvc.consume,BOOT,topics['csv'],group,183,5)['processed']==183
    assert call('csv_resume_empty',csvc.consume,BOOT,topics['csv'],group,0,3)['processed']==0
    assert call('csv_newgroup_replay',csvc.consume,BOOT,topics['csv'],PREFIX+'_replay',200,5)['processed']==200
    call('dirty_csv_produce',csvp.produce,dirty,BOOT,topics['dirty_csv'],0)
    dirty_count=call('dirty_csv_consume',csvc.consume,BOOT,topics['dirty_csv'],PREFIX+'_dirty',14,5)
    assert dirty_count=={'processed':14,'valid':7,'invalid':7}
    for n in [2,4]:results['checks']['assignments_'+str(n)]=assignments(topics['csv'],n)
    before=admin.describe_topics([topics['csv']])[0]
    admin.create_partitions({topics['csv']:NewPartitions(total_count=5)})
    after=admin.describe_topics([topics['csv']])[0]
    assert len(before['partitions'])==3 and len(after['partitions'])==5
    results['checks']['partition_increase']={'before':3,'after':5}
    assert call('json_clean_produce',jsonp.produce,clean,BOOT,topics['raw'],topics['rejected'],0)=={'accepted':200,'rejected':0}
    assert call('json_clean_route',jsonc.consume,BOOT,topics['raw'],PREFIX+'_route',topics['filtered'],topics['rejected'],200,5)=={'processed':200,'accepted':200,'rejected':0}
    routed=read_all(topics['filtered']);assert len(routed)==200
    assert {json.loads(r.value)['transaction_id'] for r in routed}==set(range(1,201))
    assert call('json_dirty_produce',jsonp.produce,dirty,BOOT,topics['dirty_raw'],topics['dirty_rejected'],0)=={'accepted':6,'rejected':8}
    rejects=[json.loads(r.value) for r in read_all(topics['dirty_rejected'])]
    assert len(rejects)==8 and all('reason' in x for x in rejects)
    results['checks']['dirty_reasons']=rejects
    sample={'transaction_id':999,'user_id':101,'amount':'1.00','timestamp':'2024-01-01 10:00:00'}
    # Versions 1 and 2 and an added optional field are accepted; removal,
    # malformed JSON, non-object JSON, invalid UTF-8 and future versions rejected.
    cases=[dict(sample,note='optional'),dict(sample,schema_version=2,currency='EUR'),
           {k:v for k,v in sample.items() if k!='amount'},b'{broken',b'[]',b'\xff',dict(sample,schema_version=3)]
    p=make_producer(BOOT)
    for event in cases:p.send(topics['schema'],value=event if isinstance(event,bytes) else json_bytes(event)).get(timeout=30)
    p.close()
    assert call('schema_evolution',jsonc.consume,BOOT,topics['schema'],PREFIX+'_schema',topics['schema_ok'],topics['schema_bad'],7,5)=={'processed':7,'accepted':2,'rejected':5}
    assert len(read_all(topics['schema_ok']))==2 and len(read_all(topics['schema_bad']))==5
    # Replay the same source with a distinct group: outputs deliberately repeat.
    assert call('json_replay',jsonc.consume,BOOT,topics['raw'],PREFIX+'_route_replay',topics['filtered'],topics['rejected'],200,5)['accepted']==200
    assert len(read_all(topics['filtered']))==400
    results['checks']['replay_output_records']=400
    results['checks']['all_checks_passed']=True
finally:
    admin.close()
    (OUT/'integration_results.json').write_text(json.dumps(results,indent=2))
print('ALL REAL-BROKER CHECKS PASSED',flush=True)
