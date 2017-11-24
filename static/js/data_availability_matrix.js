/**
 * Created by vdn73631 on 23/11/2017.
 */

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = jQuery.trim(cookies[i]);
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

function buildSelect(array, name) {
    var html, j;
    if (array[name].length === 1){
        return "<input readonly id="+ name + " name="+ name +" value="+ array[name] +">"
    }
    html = "<select style='width: 152px' id="+ name +" name=" + name + ">";
    for (j = 0; j < array[name].length; j++) {
        html = html.concat("<option>" + array[name][j] + "</option>")
    }
    html = html.concat("</select>")

    return html
}

$(".experiments").chosen({width:"95%",placeholder_text_multiple: "Choose one or more experiments"})
$(".select-expr").click(function () {
    $('.experiments option').prop('selected', true)
    $('.experiments').trigger('chosen:updated')
})
$(".deselect-expr").click(function () {
    $('.experiments option').prop('selected', false)
    $('.experiments').trigger('chosen:updated')
})

$(".variables").chosen({width: "95%",placeholder_text_multiple: "Choose one or more variables"}).change(function () {
    var data = {};
    var variables = $(".variables").val()
    data["variables"] = JSON.stringify(variables)
    var target = "/data-availability-variables"

    // Show hide the variable details table.
    if (variables.length > 0){
        $("#variable-details").show()
    }
    else {
        $("#variable-details").hide()
    }

    // POST request to retrieve variable tables and frequencies.
    $.ajax({
        method: "POST",
        url: target,
        data: data,
        success: function (returned_data) {
            var i, variable_details = "<tr>", vari;
            for (i = 0; i < returned_data.variables.length; i++) {
                vari = returned_data.variables[i];

                table_select = buildSelect(vari, "tables")
                freq_select = buildSelect(vari, "freqs")

                variable_details = variable_details.concat("<td>" + vari.variable + "</td><td>" + table_select + "</td><td>" + freq_select + "</td>")
                variable_details = variable_details.concat("</tr>")

                $(".variable-details").html(variable_details)
            }
        },
        headers: {
            'X-CSRFTOKEN': getCookie('csrftoken')
        },
        dataType: "json"
    })
})

$(".select-variable").click(function () {
    $('.variables option').prop('selected', true)
    $('.variables').trigger('chosen:updated').trigger('change')
})
$(".deselect-variable").click(function () {
    $('.variables option').prop('selected', false)
    $('.variables').trigger('chosen:updated').trigger('change')
})


function ensembleTags(array){
    var j
    var tagstring = ""
    for (j=0; j<array.length; j++){
        tagstring = tagstring.concat("<p class='ensemble-tag'>"+ array[j] + "</p>")
    }


    return tagstring
}

$('#get_results').click(function () {
    var data_results = $("#results")
    data_results.fadeTo('fast',0.5)
    var datastring = $("#filterform").serialize();
    var target = "/data-availability/"
    $.ajax({
        type: "POST",
        url: target,
        data: datastring,
        dataType: "json",
        success: function (returnedData) {
            console.log(returnedData.length)
            if (returnedData.length < 1){
                console.log("test")
                $('#results table').hide()
                data_results.fadeTo('fast', 1)
                $('#results div').html("<h4 class='text-danger'>Your search returned no results, please edit your filters and try again.</h4>").show()

            }
            else {
                $('#results div').hide()

                var i
                var results = ""
                var institute, model, experiment, ensembles

                for (i = 0; i < returnedData.length; i++) {
                    institute = "<td>" + returnedData[i].institute + "</td>"
                    model = "<td>" + returnedData[i].model + "</td>"
                    experiment = "<td>" + returnedData[i].experiment + "</td>"
                    ensembles = "<td>" + ensembleTags(returnedData[i].ensembles) + "</td>"
                    results = results.concat("<tr>" + institute + model + experiment + ensembles + "</tr>")
                }
                $("#data-availability-results").html(results)
                // show results table
                $('#results table').show()
                data_results.fadeTo('fast', 1)
            }

        },
        error: function () {
            $('#results table').hide()
            $("#results").fadeTo('fast',1)
            $('#results div').html("<h4 class='text-danger'>Make sure you have a selection for all filters and try again.</h4>").show()
        }
    })
})

$('#clear_filters').click(function () {
    $('#model').val('All').trigger('change')
    $('#experiment').val('All').trigger('change')
})
