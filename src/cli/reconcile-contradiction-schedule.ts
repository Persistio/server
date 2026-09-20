// Compiles into the standard runtime image's dist tree. No source checkout,
// widened Docker context, container entrypoint change or provider access needed.
export async function runReconciliationCli(args=process.argv.slice(2)):Promise<void>{
  const usage='Usage: node dist/cli/reconcile-contradiction-schedule.js --workers-stopped --vault-id UUID [--after UUID --through UUID]';
  const input=[...args],values:Record<string,string>={};let invalid=input.shift()!=='--workers-stopped';
  for(let i=0;i<input.length;i+=2){
    const key=input[i],value=input[i+1];
    if(!['--vault-id','--after','--through'].includes(key)||key in values||value===undefined){invalid=true;break;}
    values[key]=value;
  }
  const uuid=/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;
  invalid ||= !uuid.test(values['--vault-id']??'')||Object.values(values).some(value=>!uuid.test(value))
    ||!!values['--after']&&!values['--through'];
  if(invalid){console.error(usage);process.exitCode=2;return;}
  // Deliberately load configuration only after the strict credential-free parser.
  let closePool:(()=>Promise<void>)|undefined;
  try{
    ({closePool}=await import('../db/client'));
    const {reconcileContradictionSchedulePage}=await import('../services/contradiction-reconciliation');
    console.log(JSON.stringify(await reconcileContradictionSchedulePage({vaultId:values['--vault-id'],
      after:values['--after'],through:values['--through']})));
  }catch(error){
    const reasons=new Set(['Invalid reconciliation cursor','Schema maintenance is active','Contradiction scheduler is active','Reconciliation vault is unavailable']);
    const message=error instanceof Error?error.message:'';
    console.error(reasons.has(message)?message:'Schedule reconciliation failed; inspect configuration and bounded server diagnostics.');
    process.exitCode=1;
  }finally{try{await closePool?.();}catch{process.exitCode=1;}}
}
if(require.main===module)void runReconciliationCli();
