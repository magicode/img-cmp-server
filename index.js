var leveldown = require("leveldown");
var imgcmp = require("img-cmp");
var onnex = require("onnex");
var async = require("async");





var DB_PATH = process.env.DB_PATH || 'test.db';//'img-vector.db';

var IP = process.env.IP || '0.0.0.0';
var PORT = process.env.PORT || 8080;

var db = leveldown(DB_PATH);



var onnexServer = onnex.create();



var VECTOR_SIZE = 512;
var HASH_SIZE = 20;

var threshold = 0.03;
var likeRange = ( 1 / 100 ) * imgcmp.maxDiff;

db.open(function(){
    
    var qMethods = {};
    
    var q = async.queue(function (task, callback) {
        if(task.method && qMethods[task.method]){
            var oldCallback = task.callback;
            task.callback = function(){
                callback();
                
                if(oldCallback) oldCallback.apply(this,arguments);
            };
            qMethods[task.method](task);
        }else{
            callback();
        }
    });
    
    function findImage( vector , sum , hash , callback ){
        q.push({ vector: vector , sum: sum , hash: hash , callback: callback , method: 'findImage' });
    }
    
    function addImage( vector, sum, hash ,callback){
        q.push({ vector: vector , sum: sum , hash: hash , callback: callback , method: 'addImage' });
    }
    
    qMethods.addImage = function qAddImage( obj ){
        var vector = obj.vector , sum = obj.sum , hash =  obj.hash ,callback = obj.callback;
        
        if(vector.length != VECTOR_SIZE)
            return callback('vector.length != VECTOR_SIZE');
            
        if(hash.length != HASH_SIZE)
            return callback('hash.length != HASH_SIZE');
            
        if(!(sum > 0 && sum < 0xffff))
            return callback('!(sum > 0 && sum < 0xffff)');
        
        
        var key = new Buffer(24);
        key.fill(0);
        
        key.writeUInt16BE(sum,1);
        hash.copy(key,4);
        
        console.log("addImage");
        
        db.put(key,vector,function(err){
            callback(err);
        });
    };
    
    qMethods.findImage = function qFindImage( obj ){
        
        var vector = obj.vector , sum = obj.sum , hash = obj.hash ,callback = obj.callback;

        if(vector.length != VECTOR_SIZE)
            return callback('vector.length != VECTOR_SIZE');
            
        if(!(sum > 0 && sum < 0xffff))
            return callback('!(sum > 0 && sum < 0xffff)');
        
        var startUpBuffer = new Buffer(3);
        startUpBuffer[0] = 0;
        startUpBuffer.writeUInt16BE(sum,1);
        
        var endUpBuffer = new Buffer(3);
        endUpBuffer[0] = 0;
        endUpBuffer.writeUInt16BE(Math.floor(Math.min(sum + likeRange,imgcmp.maxDiff)),1);
        
        
        var startDownBuffer = new Buffer(3);
        startDownBuffer[0] = 0;
        startDownBuffer.writeUInt16BE(sum-1,1);
        
        var endDownBuffer = new Buffer(3);
        endDownBuffer[0] = 0;
        endDownBuffer.writeUInt16BE(Math.floor(Math.max(sum - likeRange,0)),1);



        var iterList = [];

        iterList.push( db.iterator({ start:  startUpBuffer , end: endUpBuffer }) );
        iterList.push( db.iterator({ start:  startDownBuffer , end: endDownBuffer ,reverse: true }) );
        
        
        
        var currentIter = 1;
        var cmpCount = 0;
        function next(){
            
            
            if(!iterList.length){
                if(hash){
                    
                    obj.callback = function(err){
                        callback(err,null,true);
                    };
                    qMethods.addImage(obj);
                }else{
                    callback(null,null);
                }
                return;
            }
            
            currentIter = ( currentIter + 1 ) % iterList.length;
            
            var iterCurr = iterList[currentIter];
            
            iterCurr.next(function(err,key,value){
                
                if(key === undefined){
                    iterList.splice(currentIter,1);
                    iterCurr.end(function(){});
                    return next();
                }
                
                imgcmp.compare(value,vector,function(err,value){
                    cmpCount++;
                    if(value <= threshold){
                        var findHash = key.slice(4);
                        iterList.forEach(function(i){ i.end(function(){}); });
                        callback(null, findHash ,null,{ key: key.toString('hex') ,value: value , cmpCount: cmpCount });
                    }else{ 
                        next();
                    }
                });
                
            });
        
        }
        
        next();
    };
    
    
    onnexServer.addFunction("findImageElseAdd",function( vector, sum, hash){
        var cb = Array.prototype.slice.call(arguments).pop();
       
        vector = new Buffer(vector,'hex');
        hash = new Buffer(hash,'hex');
        
        findImage( vector, sum , hash , function(err , findHash ,info ,add ){
            cb(err , findHash && findHash.toString('hex') , info , add);
        });
        
       
    });
   
    onnexServer.addBind({ port: PORT , host: IP });


});
