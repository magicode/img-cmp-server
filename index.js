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


var thresholdPercent = process.env.THRESHOLD || 2;

var threshold = (thresholdPercent / 100);
var likeRange = ( 1 / 100 ) * imgcmp.maxDiff;

db.open(function(){
    
    var qMethods = {};
    
    var q = async.queue(function (task, callback) {
        if(task.method && qMethods[task.method]){
            qMethods[task.method](task,callback);
        }else{
            callback();
        }
    });
    
    function findImage( vector , sum , hash , callback ){
        q.push({ vector: vector , sum: sum , hash: hash , callback: callback , method: 'findImage' },callback);
    }
    
    function addImage( vector, sum, hash ,callback){
        q.push({ vector: vector , sum: sum , hash: hash , callback: callback , method: 'addImage' },callback);
    }
    
    qMethods.addImage = function qAddImage( obj ,callback){
        var vector = obj.vector , sum = obj.sum , hash =  obj.hash ;//,callback = obj.callback;
        
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
    
    qMethods.findImage = function qFindImage( obj ,callback){
        
        var vector = obj.vector , sum = obj.sum , hash = obj.hash ;//,callback = obj.callback;

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
                    qMethods.addImage(obj ,function(err){
                        callback(err,null,true,{ cmpCount: cmpCount });
                    });
                }else{
                    callback(null,null,false,{ cmpCount: cmpCount });
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
                var val = imgcmp.compare(value,vector);
                cmpCount++;
                
                if(val <= threshold){
                    var findHash = key.slice(4);
                    iterList.forEach(function(i){ i.end(function(){}); });
                    callback(null, findHash ,null,{ key: key.toString('hex') ,value: val , cmpCount: cmpCount });
                }else{ 
                    next();
                }
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


    onnexServer.addFunction("findImage", function(vector, sum) {
        var cb = Array.prototype.slice.call(arguments).pop();

        vector = new Buffer(vector, 'hex');

        qMethods.findImage({
            vector: vector,
            sum: sum
        },
        function(err, findHash, info, add) {
            cb(err, findHash && findHash.toString('hex'), info, add);
        });

    });
    
    onnexServer.addBind({ port: PORT , host: IP });


});
