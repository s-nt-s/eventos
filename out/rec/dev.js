// ============================================================
// CAPTURA DE TODOS LOS LOGS Y ERRORES PARA DEPURACIÓN EN MOBILE
// ============================================================
(function() {
    // Bloqueo para evitar bucles infinitos (alert llama a console.log, etc.)
    var alertLock = false;
    var originalAlert = window.alert;

    function safeAlert(msg) {
        if (alertLock) return;
        alertLock = true;
        try {
            originalAlert(msg);
        } catch(e) {
            // Si el alert falla, ignorar
        }
        alertLock = false;
    }

    // 1. Capturar errores no capturados (window.onerror)
    window.onerror = function(message, source, lineno, colno, error) {
        var details = [
            '🚨 ERROR NO CAPTURADO',
            'Mensaje: ' + message,
            'Archivo: ' + source,
            'Línea: ' + lineno + ':' + colno
        ];
        if (error && error.stack) {
            details.push('Pila: ' + error.stack);
        }
        safeAlert(details.join('\n'));
        return false; // No suprimimos el error, también va a consola nativa
    };

    // 2. Capturar promesas rechazadas no manejadas
    window.addEventListener('unhandledrejection', function(event) {
        var reason = event.reason;
        var msg = '⚠️ PROMESA RECHAZADA NO MANEJADA\n';
        if (reason instanceof Error) {
            msg += reason.message + '\n' + reason.stack;
        } else {
            msg += String(reason);
        }
        safeAlert(msg);
    });

    // 3. Sobrescribir console.log para mostrar alert
    var originalLog = console.log;
    console.log = function() {
        var args = Array.from(arguments);
        var msg = '📝 LOG: ' + args.join(' ');
        safeAlert(msg);
        originalLog.apply(console, arguments);
    };

    // 4. Sobrescribir console.error para mostrar alert
    var originalError = console.error;
    console.error = function() {
        var args = Array.from(arguments);
        var msg = '❌ ERROR: ' + args.join(' ');
        safeAlert(msg);
        originalError.apply(console, arguments);
    };

    // (Opcional) Sobrescribir console.warn si quieres
    var originalWarn = console.warn;
    console.warn = function() {
        var args = Array.from(arguments);
        safeAlert('⚠️ WARN: ' + args.join(' '));
        originalWarn.apply(console, arguments);
    };

    // Mensaje de inicio para confirmar que se activó
    safeAlert('✅ CAPTURA DE LOGS Y ERRORES ACTIVADA. Todos los logs y errores aparecerán aquí.');
})();