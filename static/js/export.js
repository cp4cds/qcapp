function downloadCSV(csv, filename) {
    var csvFile;
    var downloadLink;

    // CSV file
    csvFile = new Blob([csv], {type: "text/csv"});

    // Download link
    downloadLink = document.createElement("a");

    // File name
    downloadLink.download = filename;

    // Create a link to the file
    downloadLink.href = window.URL.createObjectURL(csvFile);

    // Hide download link
    downloadLink.style.display = "none";

    // Add the link to DOM
    document.body.appendChild(downloadLink);

    $('#downloadModal').modal('hide');

    // Click download link
    downloadLink.click();
}

function downloadJSON(JSON, filename) {
    var csvFile;
    var downloadLink;

    // CSV file
    jsonFile = new Blob([JSON], {type: "text/json"});

    // Download link
    downloadLink = document.createElement("a");

    // File name
    downloadLink.download = filename;

    // Create a link to the file
    downloadLink.href = window.URL.createObjectURL(jsonFile);

    // Hide download link
    downloadLink.style.display = "none";

    // Add the link to DOM
    document.body.appendChild(downloadLink);

    $('#downloadModal').modal('hide');

    // Click download link
    downloadLink.click();
}

function makeRow(JSON){
    var results_array = [];
    for (i=0; i<JSON.length; i++){
        var row = [];

        // Add institute
        row.push(JSON[i].institute);

        // Add Model
        row.push(JSON[i].model);

        // Add Experiment
        row.push(JSON[i].experiment);

        // Add Ensembles
        var ensembles = JSON[i].ensembles;
        row = row.concat(ensembles);

        results_array.push(row.join(","));
    }

    return results_array
}

function objToStr(object, key){
    return [key,object[key]].join(',')
}

function jsonToCSV(JSON, filename){
    var csv = [];
    // Build header
        // Push provenance
        csv.push("Provenance");
        csv.push(objToStr(JSON.provenance,'source'));
        csv.push(objToStr(JSON.provenance,'access_date'));
        csv.push(objToStr(JSON.provenance,'version'));

        // Push query
        csv.push("Search Query - Variables | Tables | Frequencies are read vertically where the variable was searched using the table and frequency immediately below");
        var query = JSON.query;


        // add variable
        query.variables.unshift('Variables');
        csv.push(query.variables.join(","));

        // add tables
        query.tables.unshift('Tables');
        csv.push(query.tables.join(","));

        // add freqs
        query.frequencies.unshift('Frequencies');
        csv.push(query.frequencies.join(","));

        // add experiments
        query.experiments.unshift('Experiments');
        csv.push(query.experiments.join(","));

        // add min_ensembles
        csv.push(["Minimum Ensemble Size", query.ensemble_size.join(",")]);

    // Push results header
    csv.push("Results");
    csv.push(["Institute","Model","Experiment","Ensembles"].join(","));

    // Push results
    csv = csv.concat(makeRow(JSON.results));

    downloadCSV(csv.join("\n"), filename)

}

function exportResltsToJSON(form_id, target_url, filename) {
    $('#downloadModal').modal('show');

    // Get the data from the form
    var datastring = $("#" + form_id).serialize();

    $.ajax({
        type: "POST",
        url: target_url,
        data: datastring,
        dataType: "json",
        success: function(data) {
            downloadJSON(JSON.stringify(data), filename)
        }
    })
}

function exportResltsToCSV(form_id, target_url, filename) {
    $('#downloadModal').modal('show');

    // Get the data from the form
    var datastring = $("#" + form_id).serialize();

    $.ajax({
        type: "POST",
        url: target_url,
        data: datastring,
        dataType: "json",
        success: function(data) {
            jsonToCSV(data, filename)
        }
    })
}