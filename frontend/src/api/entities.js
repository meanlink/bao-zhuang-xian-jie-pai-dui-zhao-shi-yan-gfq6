import {api} from './client.js';
export const datasets={list:()=>api('/datasets'),get:id=>api('/datasets/'+id),records:(id,page=1,page_size=50)=>api('/datasets/'+id+'/records?page='+page+'&page_size='+page_size),create:data=>api('/datasets',{method:'POST',body:JSON.stringify(data)})};
export const uploads={
 list:()=>api('/uploads'),
 create:data=>api('/uploads',{method:'POST',body:JSON.stringify(data)}),
 get:id=>api('/uploads/'+id),
 putChunk:(id,seq,blob)=>api('/uploads/'+id+'/chunks/'+seq,{method:'PUT',body:blob,headers:{'Content-Type':'application/octet-stream'}}),
 complete:id=>api('/uploads/'+id+'/complete',{method:'POST'}),
 parse:id=>api('/uploads/'+id+'/parse',{method:'POST'}),
 rows:(id,page=1,page_size=50)=>api('/uploads/'+id+'/rows?page='+page+'&page_size='+page_size),
};
export const jobs={list:()=>api('/jobs'),get:id=>api('/jobs/'+id),create:data=>api('/jobs',{method:'POST',body:JSON.stringify(data)}),approve:id=>api('/jobs/'+id+'/approve',{method:'POST'})};
export const reports={list:()=>api('/reports'),create:data=>api('/reports',{method:'POST',body:JSON.stringify(data)}),toggle:id=>api('/reports/'+id+'/toggle',{method:'POST'}),deliveries:()=>api('/deliveries')};
