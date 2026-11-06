import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.6.0/index.js';

// Configuración de escenarios y umbrales de rendimiento (Thresholds)
export const options = {
  stages: [
    { duration: '30s', target: 20 },  // Ramp-up: Sube gradualmente a 20 usuarios virtuales
    { duration: '1m', target: 50 },   // Carga sostenida (Estrés moderado): 50 usuarios concurrentes
    { duration: '30s', target: 150 }, // Pico de estrés masivo: 150 usuarios concurrentes
    { duration: '30s', target: 0 },   // Ramp-down: Descenso a 0 usuarios
  ],
  thresholds: {
    http_req_duration: ['p(95)<300'], // El 95% de las peticiones HTTP deben responder en menos de 300ms
    http_req_failed: ['rate<0.01'],    // La tasa de errores debe ser menor al 1%
  },
};

export default function () {
  const url = 'http://localhost:8081/api/v1/notifications';
  
  // Generamos IDs únicos para evitar colisiones de idempotencia indeseadas si testeamos procesamiento único,
  // o podemos repetir claves si queremos probar la efectividad del bloqueo de Redis (SetNX).
  const uniqueId = `load-test-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  
  const payload = JSON.stringify({
    id: uniqueId,
    canal: 'email',
    destinatario: `user_${Math.floor(Math.random() * 5000)}@empresa.com`,
    mensaje: `Carga automatizada k6 - ${randomString(15)}`
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Request-ID': `k6-trace-${uniqueId}`, // Inyectamos trazabilidad personalizada
    },
  };

  const res = http.post(url, payload, params);

  // Validaciones del contrato de la API
  check(res, {
    'status es 202 Accepted': (r) => r.status === 202,
    'respuesta contiene éxito': (r) => r.json('status') === 'success',
  });

  sleep(0.2); // Pequeña pausa de 200ms por usuario virtual
}