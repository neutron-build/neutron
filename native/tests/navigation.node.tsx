import { test } from 'node:test'
import assert from 'node:assert/strict'
import React, { createElement, useState } from 'react'
import { renderToString, renderToPipeableStream } from 'react-dom/server'
import { Writable } from 'node:stream'
import { Stack } from '../packages/neutron-native/src/navigation/Stack.native.tsx'
import { Tabs } from '../packages/neutron-native/src/navigation/Tabs.native.tsx'
import { buildRouteTree, matchRoute } from '../packages/neutron-native/src/router/file-discovery.ts'
import { navigate, replace, goBack, routerState } from '../packages/neutron-native/src/router/navigator.ts'
const Screen=()=>createElement('p',null,'visible-screen')
test('documented JSX registers screens through real React rendering',()=>{
  const jsx=createElement(Stack,{initialRouteName:'home'},createElement(Stack.Screen,{name:'home',component:Screen}))
  assert.match(renderToString(jsx),/visible-screen/)
})
test('independent navigators do not share declarations; nested depth selects nested route',()=>{
  const Other=()=>createElement('p',null,'other-screen')
  navigate('/home')
  assert.match(renderToString(createElement(Stack,{},createElement(Stack.Screen,{name:'home',component:Other}))),/other-screen/)
  assert.match(renderToString(createElement(Stack,{},createElement(Stack.Screen,{name:'home',component:Screen}))),/visible-screen/)
  const Nested=()=>createElement(Tabs,{initialRouteName:'child'},createElement(Tabs.Screen,{name:'child',component:Screen}))
  navigate('/home/child')
  assert.match(renderToString(createElement(Stack,{},createElement(Stack.Screen,{name:'home',component:Nested}))),/visible-screen/)
})
test('programmatic push/replace/back synchronously share router history',()=>{
  navigate('/home'); navigate('/detail'); replace('/replacement'); goBack(); assert.equal(routerState.value.pathname,'/home')
})
test('discovery executes neither hook components nor loaders and static routes win',()=>{
  let executions=0,loads=0
  const Hooks=()=>{useState(0);executions++;return createElement('p',null,'hook-screen')}
  const manifest={entries:[{path:'[id]',segment:'[id]',dynamic:true,group:false,layout:false,notFound:false},{path:'home',segment:'home',dynamic:false,group:false,layout:false,notFound:false}]}
  const tree=buildRouteTree(manifest,{'[id]':Hooks,home:{kind:'lazy',load:async()=>{loads++;return {default:Screen}}}})
  assert.equal(executions,0);assert.equal(loads,0);assert.equal(matchRoute(['home'],tree)?.record.segment,'home')
})
test('declared lazy route loads and renders through actual Suspense',async()=>{
  let loads=0
  const tree=buildRouteTree({entries:[{path:'home',segment:'home',dynamic:false,group:false,layout:false,notFound:false}]},{home:{kind:'lazy',load:async()=>{loads++;return {default:Screen}}}})
  assert.equal(loads,0)
  const Route=tree[0].component!
  let html=''
  await new Promise<void>((resolve,reject)=>{
    const sink=new Writable({write(chunk,_encoding,callback){html+=chunk.toString();callback()}})
    sink.on('finish',resolve);sink.on('error',reject)
    const stream=renderToPipeableStream(createElement(Route),{onAllReady(){stream.pipe(sink)},onError:reject})
  })
  assert.equal(loads,1);assert.match(html,/visible-screen/)
})

test('animated styles fail explicitly without Reanimated',async()=>{
  const {useAnimatedStyle}=await import('../packages/neutron-native/src/animated/worklets.ts')
  assert.throws(()=>useAnimatedStyle(()=>({opacity:1})),/react-native-reanimated/)
})
