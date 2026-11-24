import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import transaction, csv_rows, row_event, strict_json_loads

BASE = dict(transaction_id='1', user_id='101', amount='19.99', timestamp='2024-01-01 10:00:05')


class SchemaTests(unittest.TestCase):
    def test_clean_source(self):
        path=Path(__file__).resolve().parents[1]/'csv_lab/data/transactions.csv'
        events=[row_event(row) for _,row,_ in csv_rows(path)]
        self.assertEqual(len(events),200)
        self.assertEqual(len({e['transaction_id'] for e in events}),200)

    def test_dirty_source(self):
        path=Path(__file__).resolve().parents[1]/'csv_lab/data/transactions_dirty.csv'
        valid=[]; invalid=[]
        for line,row,_ in csv_rows(path):
            try:valid.append(row_event(row))
            except ValueError:invalid.append(line)
        self.assertEqual(len(invalid),7)
        self.assertEqual(len(valid),7)
        self.assertEqual(len({e['transaction_id'] for e in valid}),6)

    def test_optional_fields_compatible(self):
        self.assertEqual(transaction(dict(BASE,currency='EUR',note='new field'))['note'],'new field')

    def test_version_2_default(self):
        self.assertEqual(transaction(dict(BASE,schema_version=2))['currency'],'EUR')

    def test_nonstandard_json_constants(self):
        for raw in ['{"extra":NaN}', '{"extra":Infinity}']:
            with self.assertRaises(ValueError):strict_json_loads(raw)

    def test_removed_required_field(self):
        event=BASE.copy();del event['amount']
        with self.assertRaises(ValueError):transaction(event)

    def test_reject_invalid_values(self):
        for field,value in [('amount','NaN'),('amount','Infinity'),('amount',True),
                            ('amount','1.001'),('amount','-1'),('user_id',False),
                            ('transaction_id','1.5'),('timestamp','not-a-date'),
                            ('schema_version',99),('schema_version',True)]:
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):
                transaction(dict(BASE,**{field:value}))


if __name__=='__main__':unittest.main()
