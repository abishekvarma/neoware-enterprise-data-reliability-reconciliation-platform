from pathlib import Path
import json,pandas as pd
BASE=Path(__file__).resolve().parents[1]; RAW=BASE/'data/raw'; PROCESSED=BASE/'data/processed'; OUTPUT=BASE/'data/output'
for p in [PROCESSED,OUTPUT]: p.mkdir(parents=True,exist_ok=True)
def run_pipeline():
 c=pd.read_csv(RAW/'customer_master.csv'); o=pd.DataFrame(json.loads((RAW/'orders_api.json').read_text())); i=pd.read_csv(RAW/'legacy_invoices.csv'); s=pd.read_csv(RAW/'support_system.csv')
 for df in [c,o,i,s]: df.columns=[x.strip().lower() for x in df.columns]
 o['order_amount']=pd.to_numeric(o.order_amount,errors='coerce'); i['invoice_amount']=pd.to_numeric(i.invoice_amount,errors='coerce')
 o['order_date']=pd.to_datetime(o.order_date,errors='coerce'); i['invoice_date']=pd.to_datetime(i.invoice_date,errors='coerce'); s['ticket_date']=pd.to_datetime(s.ticket_date,errors='coerce')
 for df in [c,o,i,s]:
  for col in df.select_dtypes('object').columns: df[col]=df[col].astype(str).str.strip()
 for name,df in [('customers',c),('orders',o),('invoices',i),('support',s)]: df.to_csv(PROCESSED/f'silver_{name}.csv',index=False)
 issues=[]
 def add(src,key,rule,sev,detail): issues.append([src,key,rule,sev,detail])
 valid_customers=set(c.customer_id)
 for _,r in o.iterrows():
  if pd.isna(r.order_amount) or r.order_amount<=0: add('orders_api',r.order_id,'positive_order_amount','High','order_amount must be > 0')
  if pd.isna(r.order_date): add('orders_api',r.order_id,'valid_order_date','High','order_date could not be parsed')
  if r.customer_id not in valid_customers: add('orders_api',r.order_id,'customer_reference','Critical','customer_id not found in master')
 for _,r in i.iterrows():
  if pd.isna(r.invoice_amount) or r.invoice_amount<=0: add('legacy_invoices',r.invoice_id,'positive_invoice_amount','High','invoice_amount must be > 0')
 for _,r in i[i.duplicated('order_id',keep=False)].iterrows(): add('legacy_invoices',r.invoice_id,'unique_invoice_order','High',f"order_id {r.order_id} appears multiple times in invoice source")
 q=pd.DataFrame(issues,columns=['source','record_key','rule','severity','detail'])
 recon=o.groupby('order_id',as_index=False).order_amount.sum().merge(i.groupby('order_id',as_index=False).invoice_amount.sum(),on='order_id',how='outer',indicator=True)
 recon[['order_amount','invoice_amount']]=recon[['order_amount','invoice_amount']].fillna(0); recon['difference']=(recon.order_amount-recon.invoice_amount).round(2)
 recon['status']=recon.apply(lambda r:'Matched' if abs(r.difference)<.01 and r._merge=='both' else ('Missing Invoice' if r._merge=='left_only' else ('Missing Order' if r._merge=='right_only' else 'Mismatch')),axis=1)
 recon=recon.drop(columns='_merge')
 trusted=o[o.order_id.isin(recon.loc[recon.status=='Matched','order_id'])].merge(c,on='customer_id',how='left'); trusted['record_status']='TRUSTED'
 health=[]
 for n,df,key in [('customer_master',c,'customer_id'),('orders_api',o,'order_id'),('legacy_invoices',i,'invoice_id'),('support_system',s,'ticket_id')]:
  score=round(df.notna().mean().mean()*100,1); health.append([n,len(df),score,df[key].nunique(),'Healthy' if score>=98 else 'Review'])
 h=pd.DataFrame(health,columns=['source','records','completeness_pct','unique_keys','status'])
 q.to_csv(OUTPUT/'quality_issues.csv',index=False); recon.to_csv(OUTPUT/'reconciliation_report.csv',index=False); trusted.to_csv(OUTPUT/'trusted_orders.csv',index=False); h.to_csv(OUTPUT/'source_health.csv',index=False)
 matched=int((recon.status=='Matched').sum()); quality=round(max(0,100-len(q)/max(len(o)+len(i),1)*100),1)
 summary={'raw_records':len(c)+len(o)+len(i)+len(s),'trusted_orders':len(trusted),'reconciliation_records':len(recon),'reconciliation_match_rate_pct':round(matched/max(len(recon),1)*100,1),'quality_score_pct':quality,'quality_issues':len(q),'sources':4}
 (OUTPUT/'pipeline_summary.json').write_text(json.dumps(summary,indent=2)); (OUTPUT/'quality_report.md').write_text(f"# Pipeline Quality Report\n\nRaw records: **{summary['raw_records']}**\n\nTrusted orders: **{summary['trusted_orders']}**\n\nReconciliation match rate: **{summary['reconciliation_match_rate_pct']}%**\n\nQuality score: **{summary['quality_score_pct']}%**\n\nQuality issues: **{summary['quality_issues']}**\n")
 return summary
if __name__=='__main__': print(json.dumps(run_pipeline(),indent=2))
