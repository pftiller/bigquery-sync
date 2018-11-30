

function pivotData(data) {
    
var fields = ["link_click","page_engagement","post_engagement"];

var o = arr.reduce( (a,b, c) => {
    a[b.campaign_name] = a[b.campaign_name] || [];
    a[b.campaign_name].push({[b.actions__action_type]:b.actions__value});
    return a;
}, {});

var a = Object.keys(o).map(function(k) {
	var m = Object.assign.apply({},o[k]);
    fields.forEach( (x) => { if ( !(x in m) ) m[x] = 0 });
    
    return {campaign : k, month : m};
});
    
    console.log(a);
}


module.exports = {
    pivotData: pivotData
}