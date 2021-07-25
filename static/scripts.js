Dropzone.options.uploadForm = {
    success: function(file, response) {

        var info = (response.info) ? response.info : '';
        document.getElementById('info').innerHTML = info;

        var download = (response.download) ? '<a class="button" href="' + response.download + '">Download filtered SDMX data</a>' : '';
        document.getElementById('download').innerHTML = download;

        var summary = '';
        if (response.removed < response.total) {
            summary += '<h2>Results:</h2>';
            summary += '<p>' + response.removed + ' series keys were removed (out of ' + response.total + ').</p>';
        }
        document.getElementById('summary').innerHTML = summary;

        var messages = '';
        if (response.messages && response.messages.length > 0) {
            messages += '<h2>Reasons for removals:</h2>';
            messages += '<ol>';
            response.messages.forEach(function(message) {
                messages += '<li>' + message + '</li>';
            });
            messages += '</ol>';
        }
        document.getElementById('messages').innerHTML = messages;
    }
}