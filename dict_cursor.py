class DictCursor(object):
 def __init__(self,cursor):
     object.__setattr__(self,'__cursor',cursor)
     object.__setattr__(self,'__dirty',True)
     object.__setattr__(self,'__description',[])

 def __getattribute__(self,x):
     try:
         return object.__getattribute__(self,x)
     except AttributeError:
         cursor = object.__getattribute__(self,'__cursor')
         return getattr(cursor,x)

 def __setattr__(self,x,v):
     setattr(self.__cursor,x,v)

 def fetchone(self):
     cursor = object.__getattribute__(self,'__cursor')
     dirty = object.__getattribute__(self,'__dirty')
     r = cursor.fetchone()
     if dirty is True:
         description = [x[0] for x in cursor.description]
         object.__setattr__(self,'__description',description)
         object.__setattr__(self,'__dirty',False)
     else:
         description = object.__getattribute__(self,'__description')
     if r is not None:
         return dict(zip(description,r))
     return r

 def fetchall(self):
     cursor = object.__getattribute__(self,'__cursor')
     dirty = object.__getattribute__(self,'__dirty')
     l = cursor.fetchall()
     if dirty is True:
         description = [x[0] for x in cursor.description]
         object.__setattr__(self,'__description',description)
         object.__setattr__(self,'__dirty',False)
     else:
         description = object.__getattribute__(self,'__description')
     for count,x in enumerate(l):
         l[count] = dict(zip(description,x))
     return l

 def execute(self,*args,**kwargs):
     object.__setattr__(self,'__dirty',True)
     cursor = object.__getattribute__(self,'__cursor')
     return cursor.execute(*args,**kwargs)

 def __iter__(self):
     return self

 def next(self):
     x = self.fetchone()
     if x is None:
         raise StopIteration
     return x
