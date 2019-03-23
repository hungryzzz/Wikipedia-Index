$(function(){
    $("#searchbtn").click(function(){
        $.getJSON("tf_idf.json", function(data){
            var $page = $("#pages");
            var key = $("#word").val().toLowerCase();
            var answer = "<ul class=\"mytheme list-group\">";
            var flag = 0;
            $page.empty();
            $.each(data, function(infoIndex, info){
                var a = info[key];
                if(typeof(a) != "undefined"){
                    var len = a.length;
                    //answer += "<h1><span class=\"mytheme label label-sample\">Sum : " + len.toString() + "</span></h1>"
                    answer += "<li class=\"list-group-item\"><strong>Sum : " + len.toString() + "</strong></li>";
                    for(var i = 0; i < len; i++){
                        answer += "<li class=\"list-group-item\"><strong>id = </strong>" + a[i].split("#", 1) + "</li>";
                    }
                    flag = 1;
                }
            })
            if(flag == 0){
                answer = "<div class=\"alert alert-warning\" role=\"alert\"><strong class=\"mytheme\">NOTHING!</strong></div>";
            }
            else{
                answer += "</ul>";
            }
            $page.html(answer);
        })
    })
})
