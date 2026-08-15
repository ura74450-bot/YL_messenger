self.addEventListener('install',event=>{self.skipWaiting();});
self.addEventListener('activate',event=>{event.waitUntil(self.clients.claim());});
self.addEventListener('push',event=>{
  let data={}; try{data=event.data?event.data.json():{};}catch{data={title:'YL Messenger',body:'Новое сообщение'};}
  const title=data.title||'YL Messenger';
  const options={body:data.body||'Новое сообщение',icon:'/icon.svg',badge:'/icon.svg',tag:'yl-message',renotify:true,data:data.data||{}};
  event.waitUntil(self.registration.showNotification(title,options));
});
self.addEventListener('notificationclick',event=>{
  event.notification.close();
  event.waitUntil((async()=>{
    const list=await clients.matchAll({type:'window',includeUncontrolled:true});
    for(const c of list){if('focus' in c){await c.focus();return;}}
    if(clients.openWindow) await clients.openWindow('/');
  })());
});
