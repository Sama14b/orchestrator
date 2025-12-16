// orchestrator/server.js
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// URLs de los microservicios
const ACQUIRE_URL = process.env.ACQUIRE_URL || 'http://acquire:3001';
const PREDICT_URL = process.env.PREDICT_URL || 'http://predict2:3002';

app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'orchestrator',
    timestamp: new Date().toISOString()
  });
});

/**
 * Endpoint principal del orquestador
 * POST /run
 * Coordina el flujo: Acquire (obtiene datos de Kunna) → Predict (predice)
 */
app.post('/run', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('\n===========================================');
    console.log('ORQUESTADOR: Iniciando flujo de predicción');
    console.log('===========================================');

    // PASO 1: Llamar a api-acquire
    // Este servicio se conecta a Kunna, obtiene datos y genera features
    console.log('\n PASO 1: Llamando a api-acquire...');
    console.log(`   URL: ${ACQUIRE_URL}/data`);
    
    const acquireStartTime = Date.now();
    const acquireResponse = await axios.post(
      `${ACQUIRE_URL}/data`,
      req.body, // Se pasa el body por si hay parámetros adicionales
      {
        timeout: 20000, // 20 segundos para dar tiempo a Kunna
        headers: { 'Content-Type': 'application/json' }
      }
    );
    const acquireTime = Date.now() - acquireStartTime;

    console.log(`Acquire respondió en ${acquireTime}ms`);
    console.log('   Datos recibidos:', {
      dataId: acquireResponse.data.dataId,
      featureCount: acquireResponse.data.featureCount,
      features: acquireResponse.data.features
    });

    // Verificar que acquire devolvió features válidas
    if (!acquireResponse.data.features || !Array.isArray(acquireResponse.data.features)) {
      throw new Error('ACQUIRE_NO_FEATURES: Acquire no devolvió features válidas');
    }

    if (acquireResponse.data.features.length !== 7) {
      throw new Error(`ACQUIRE_WRONG_FEATURES: Se esperan 7 features, se recibieron ${acquireResponse.data.features.length}`);
    }

    const { dataId, features, featureCount, scalerVersion, createdAt } = acquireResponse.data;

    // PASO 2: Llamar a api-predict con las features obtenidas
    console.log('\n PASO 2: Llamando a api-predict...');
    console.log(`   URL: ${PREDICT_URL}/predict`);
    console.log('   Features:', features);

    const predictStartTime = Date.now();
    const predictResponse = await axios.post(
      `${PREDICT_URL}/predict`,
      {
        features: features,
        meta: {
          dataId: dataId,
          source: 'orchestrator',
          featureCount: featureCount,
          scalerVersion: scalerVersion,
          acquireTimestamp: createdAt
        }
      },
      {
        timeout: 15000,
        headers: { 'Content-Type': 'application/json' }
      }
    );
    const predictTime = Date.now() - predictStartTime;

    console.log(`Predict respondió en ${predictTime}ms`);
    console.log('   Predicción:', predictResponse.data.prediction);

    // PASO 3: Construir respuesta según el contrato
    const totalTime = Date.now() - startTime;
    
    // Respuesta según el contrato oficial
    const result = {
      dataId: dataId,
      predictionId: predictResponse.data.predictionId,
      prediction: predictResponse.data.prediction,
      timestamp: predictResponse.data.timestamp || new Date().toISOString()
    };

    console.log('\n FLUJO COMPLETADO EXITOSAMENTE');
    console.log(` Tiempo total: ${totalTime}ms`);
    console.log('Respuesta:', result);
    console.log('===========================================\n');

    res.status(200).json(result);

  } catch (error) {
    const totalTime = Date.now() - startTime;
    console.error('\n ERROR EN ORQUESTADOR');
    console.error('===========================================');
    console.error('Tipo:', error.code || 'UNKNOWN');
    console.error('Mensaje:', error.message);
    console.error('Tiempo hasta error:', `${totalTime}ms`);
    
    // Manejo detallado de errores según el origen
    
    // Error de conexión (servicio no disponible)
    if (error.code === 'ECONNREFUSED') {
      const serviceName = error.address?.includes('3001') ? 'acquire' : 'predict2';
      console.error(`Servicio no disponible: ${serviceName}`);
      return res.status(503).json({
        success: false,
        error: 'Servicio no disponible',
        detail: `No se pudo conectar a ${serviceName}`,
        service: serviceName,
        endpoint: `${error.address}:${error.port}`,
        time: `${totalTime}ms`
      });
    }

    // Timeout
    if (error.code === 'ECONNABORTED' || error.message.includes('timeout')) {
      const serviceName = error.config?.url?.includes('3001') ? 'acquire' : 'predict2';
      console.error(`Timeout en servicio: ${serviceName}`);
      return res.status(504).json({
        success: false,
        error: 'Timeout',
        detail: `El servicio ${serviceName} no respondió a tiempo`,
        service: serviceName,
        time: `${totalTime}ms`
      });
    }

    // Error de respuesta del microservicio
    if (error.response) {
      const serviceName = error.config?.url?.includes('3001') ? 'acquire' : 'predict2';
      console.error(`Error ${error.response.status} de ${serviceName}:`, error.response.data);
      return res.status(error.response.status).json({
        success: false,
        error: `Error en ${serviceName}`,
        detail: error.response.data,
        service: serviceName,
        statusCode: error.response.status,
        time: `${totalTime}ms`
      });
    }

    // Error de validación o lógica
    if (error.message.includes('ACQUIRE_') || error.message.includes('PREDICT_')) {
      console.error('Error de validación:', error.message);
      return res.status(400).json({
        success: false,
        error: 'Error de validación',
        detail: error.message,
        time: `${totalTime}ms`
      });
    }

    // Error genérico
    console.error('Error no categorizado:', error);
    console.error('===========================================\n');
    res.status(500).json({
      success: false,
      error: 'Error interno del orquestador',
      detail: error.message,
      time: `${totalTime}ms`
    });
  }
});

/**
 * GET /status
 * Verifica el estado de todos los servicios
 */
app.get('/status', async (req, res) => {
  const status = {
    orchestrator: {
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString()
    },
    services: {}
  };

  // Verificar Acquire
  try {
    const acquireHealth = await axios.get(`${ACQUIRE_URL}/health`, { timeout: 3000 });
    status.services.acquire = {
      status: 'ok',
      url: ACQUIRE_URL,
      response: acquireHealth.data
    };
  } catch (error) {
    status.services.acquire = {
      status: 'error',
      url: ACQUIRE_URL,
      error: error.message
    };
  }

  // Verificar Predict
  try {
    const predictHealth = await axios.get(`${PREDICT_URL}/health`, { timeout: 3000 });
    status.services.predict = {
      status: 'ok',
      url: PREDICT_URL,
      response: predictHealth.data
    };
  } catch (error) {
    status.services.predict = {
      status: 'error',
      url: PREDICT_URL,
      error: error.message
    };
  }

  // Determinar estado general
  const allHealthy = Object.values(status.services).every(s => s.status === 'ok');
  status.overall = allHealthy ? 'healthy' : 'degraded';

  res.json(status);
});

/**
 * GET /
 * Información del servicio
 */
app.get('/', (req, res) => {
  res.json({
    service: 'orchestrator',
    version: '1.0.0',
    description: 'Orquestador de microservicios: Acquire (Kunna) → Predict',
    endpoints: {
      '/health': 'Health check del orquestador',
      '/status': 'Estado de todos los servicios',
      '/run': 'POST - Ejecutar flujo completo de predicción'
    },
    configuration: {
      acquireUrl: ACQUIRE_URL,
      predictUrl: PREDICT_URL
    }
  });
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log('\n===========================================');
  console.log('ORQUESTADOR INICIADO');
  console.log('===========================================');
  console.log(`Puerto: ${PORT}`);
  console.log(`Acquire URL: ${ACQUIRE_URL}`);
  console.log(`Predict URL: ${PREDICT_URL}`);
  console.log('===========================================\n');
});