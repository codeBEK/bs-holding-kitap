 'use strict';
const {Pool}=require('pg');
const {AsyncLocalStorage}=require('node:async_hooks');
if(!process.env.DATABASE_URL){console.error('DATABASE_URL environment variable is required');process.exit(1)}
const pool=new Pool({connectionString:process.env.DATABASE_URL,ssl:{rejectUnauthorized:true},max:5});
const storage=new AsyncLocalStorage();
const iso=x=>x==null?null:(x instanceof Date?x.toISOString():String(x));
const date=x=>x==null?null:(x instanceof Date?x.toISOString().slice(0,10):String(x).slice(0,10));
const db=()=>{const client=storage.getStore();if(!client)throw Error('No database transaction');return client};
async function withDatabase(fn){
 const client=await pool.connect();
 try{
  await client.query('BEGIN');
  // One writer at a time: prevents two loans taking the same final copy.
  await client.query('SELECT pg_advisory_xact_lock(20261123, 2026)');
  const result=await storage.run(client,fn);
  await client.query('COMMIT');
  return result;
 }catch(e){try{await client.query('ROLLBACK')}catch{}throw e}finally{client.release()}
}
async function read(){
 const c=db();
 const [u,b,l,r,s]=await Promise.all([
  c.query('SELECT * FROM public.users'),c.query('SELECT * FROM public.books'),
  c.query('SELECT * FROM public.loans'),c.query('SELECT * FROM public.reading_history'),
  c.query('SELECT * FROM public.book_requests')]);
 const books=b.rows.map(x=>({id:x.id,title:x.title,titleRu:x.title_ru||'',author:x.author,category:x.category||'',description:x.description||'',descriptionRu:x.description_ru||'',cover:x.cover||'',total:x.total}));
 const names=new Map(books.map(x=>[x.id,x.title]));
 return {
  users:u.rows.filter(x=>x.role!=='admin').map(x=>({id:x.id,name:x.full_name,phone:x.phone,password:JSON.parse(x.password_hash),createdAt:iso(x.created_at)})),
  books,
  loans:l.rows.map(x=>({id:x.id,bookId:x.book_id,bookTitle:names.get(x.book_id)||'',userId:x.user_id,name:x.borrower_name,phone:x.borrower_phone,reason:x.reason||'',issuedAt:iso(x.borrowed_at),dueDate:date(x.due_date),returnedAt:iso(x.returned_at)})),
  reading:r.rows.map(x=>({id:x.id,userId:x.user_id,bookId:x.book_id,bookTitle:names.get(x.book_id)||'',note:x.note||'',createdAt:iso(x.read_at)})),
  suggestions:s.rows.map(x=>({id:x.id,userId:x.user_id,title:x.title,author:x.author||'',reason:x.reason||'',status:x.status==='approved'?'reviewed':'new',createdAt:iso(x.created_at)}))
 };
}
async function save(d){
 const c=db();
 // A single transaction replaces a small library snapshot. This preserves the
 // original site's API while using durable PostgreSQL storage.
 for(const table of ['book_requests','reading_history','loans','books','users']){
  if(table==='users')await c.query("DELETE FROM public.users WHERE role='reader'");
  else await c.query('DELETE FROM public.'+table);
 }
 for(const x of d.users)await c.query('INSERT INTO public.users (id,full_name,phone,password_hash,role,created_at) VALUES ($1,$2,$3,$4,$5,$6)',[x.id,x.name,x.phone,JSON.stringify(x.password),'reader',x.createdAt]);
 for(const x of d.books)await c.query('INSERT INTO public.books (id,title,title_ru,author,category,description,description_ru,cover,total) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',[x.id,x.title,x.titleRu||'',x.author,x.category||'',x.description||'',x.descriptionRu||'',x.cover||'',x.total]);
 for(const x of d.loans)await c.query('INSERT INTO public.loans (id,book_id,user_id,borrower_name,borrower_phone,reason,borrowed_at,due_date,returned_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',[x.id,x.bookId,x.userId||null,x.name,x.phone||null,x.reason||'',x.issuedAt,x.dueDate,x.returnedAt||null]);
 for(const x of d.reading)await c.query('INSERT INTO public.reading_history (id,user_id,book_id,note,read_at) VALUES ($1,$2,$3,$4,$5)',[x.id,x.userId,x.bookId,x.note||'',x.createdAt]);
 for(const x of d.suggestions)await c.query('INSERT INTO public.book_requests (id,user_id,title,author,reason,status,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)',[x.id,x.userId,x.title,x.author||'',x.reason||'',x.status==='reviewed'?'approved':'pending',x.createdAt]);
}
module.exports={withDatabase,read,save};
