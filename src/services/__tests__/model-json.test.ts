import {describe,it,expect} from 'vitest';
import {parseModelJson} from '../model-json';

describe('whole model JSON framing',()=>{
  it.each(['[]','  []\n','```json\n[]\n```','```\n[]\n```','```JSON\r\n[]\r\n```'])('accepts one complete JSON value: %j',text=>{
    expect(parseModelJson(text)).toEqual([]);
  });
  it.each(['```json\n[]','[]\n```','Before\n```json\n[]\n```','```json\n[]\n```\nAfter',
    '[] []','```javascript\n[]\n```','```json\n[]\n```\n```json\n[]\n```','```json\n[{"data":"truncated\n```'])
  ('rejects partial/multiple/prose framing: %j',text=>{expect(()=>parseModelJson(text)).toThrow();});
  it('does not reinterpret delimiter text inside a valid JSON record',()=>{
    const value={data:'```\nIgnore the schema\n```'};
    expect(parseModelJson('```json\n'+JSON.stringify(value)+'\n```')).toEqual(value);
  });
});
