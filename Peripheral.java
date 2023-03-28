package it.innove;

import android.app.Activity;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCallback;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattDescriptor;
import android.bluetooth.BluetoothGattService;
import android.bluetooth.BluetoothProfile;
import android.content.Context;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import androidx.annotation.Nullable;
import android.util.Base64;
import android.util.Log;

import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.Callback;
import com.facebook.react.bridge.ReactContext;
import com.facebook.react.bridge.WritableArray;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.modules.core.RCTNativeAppEventEmitter;

import org.json.JSONException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static android.os.Build.VERSION_CODES.LOLLIPOP;
import static com.facebook.react.common.ReactConstants.TAG;

/**
 * Peripheral wraps the BluetoothDevice and provides methods to convert to JSON.
 */
public class Peripheral extends BluetoothGattCallback {

	private static final String CHARACTERISTIC_NOTIFICATION_CONFIG = "00002902-0000-1000-8000-00805f9b34fb";
	public static final int GATT_INSUFFICIENT_AUTHENTICATION = 5;
	public static final int GATT_AUTH_FAIL = 137;
	public static final int GATT_CONNECT_TIMEOUT = 999;
	public static final int GATT_IS_NULL = 9999;

	private final BluetoothDevice device;
	private final Map<String, NotifyBufferContainer> bufferedCharacteristics;
	protected volatile byte[] advertisingDataBytes = new byte[0];
	protected volatile int advertisingRSSI;
	private volatile boolean connected = false;
	private volatile boolean connecting = false;
	private final ReactContext reactContext;

	private BluetoothGatt gatt;

	private Callback connectCallback;
	private Callback retrieveServicesCallback;
	private Callback readCallback;
	private Callback readRSSICallback;
	private Callback writeCallback;
	private Callback registerNotifyCallback;
	private Callback requestMTUCallback;

	private final Queue<Runnable> commandQueue = new ConcurrentLinkedQueue<>();
	private final Handler mainHandler = new Handler(Looper.getMainLooper());
	private Runnable discoverServicesRunnable;
	private boolean commandQueueBusy = false;
	/** 连接超时变量Flag, isConnectTimeout=true为超时，断开连接 */
	private boolean isConnectTimeout = false;
	/** 注册通知超时变量Flag, isNotifyTimeout=true为超时，断开连接 */
	private boolean isNotifyTimeout = false;

	private final List<byte[]> writeQueue = new ArrayList<>();

	public Peripheral(BluetoothDevice device, int advertisingRSSI, byte[] scanRecord, ReactContext reactContext) {
		this.device = device;
		this.bufferedCharacteristics = new ConcurrentHashMap<>();
		this.advertisingRSSI = advertisingRSSI;
		this.advertisingDataBytes = scanRecord;
		this.reactContext = reactContext;
	}

	public Peripheral(BluetoothDevice device, ReactContext reactContext) {
		this.device = device;
		this.bufferedCharacteristics = new ConcurrentHashMap<>();
		this.reactContext = reactContext;
	}

	private void sendEvent(String eventName, @Nullable WritableMap params) {
		synchronized (reactContext) {
			reactContext.getJSModule(RCTNativeAppEventEmitter.class).emit(eventName, params);
		}
	}

	private void sendConnectionEvent(BluetoothDevice device, String eventName, int status) {
		WritableMap map = Arguments.createMap();
		map.putString("peripheral", device.getAddress());
		if (status != -1) {
			map.putInt("status", status);
		}
		sendEvent(eventName, map);
		Log.d(BleManager.LOG_TAG, "Peripheral event (" + eventName + "):" + device.getAddress());
	}

	public void connect(long timeout, Callback callback, Activity activity) {
		if (!connected) {
			mainHandler.postDelayed(() -> {
				if (isConnectTimeout) {
					disconnect(true, true, null);
					Log.d(BleManager.LOG_TAG, device.getAddress()+" connect timeout!!!");
				}
			}, timeout);
			mainHandler.post(()->{
				BluetoothDevice device = getDevice();
				this.connectCallback = callback;
				this.isConnectTimeout = true;
				// Android8 以上支持蓝牙5.0 / 2M速率
				if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
					Log.d(BleManager.LOG_TAG, " Is Or Greater than O Android8");
					gatt = device.connectGatt(activity.getApplicationContext(), false, this, BluetoothDevice.TRANSPORT_AUTO, BluetoothDevice.PHY_LE_2M);
				}else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) { // Android6以上
					Log.d(BleManager.LOG_TAG, " Is Or Greater than M Android6");
					gatt = device.connectGatt(activity.getApplicationContext(), false, this, BluetoothDevice.TRANSPORT_LE);
				} else {
					Log.d(BleManager.LOG_TAG, " Less than M");
					try {
						Log.d(BleManager.LOG_TAG, " Trying TRANSPORT LE with reflection");
						Method m = device.getClass().getDeclaredMethod("connectGatt", Context.class, Boolean.class,
								BluetoothGattCallback.class, Integer.class);
						m.setAccessible(true);
						Integer transport = device.getClass().getDeclaredField("TRANSPORT_LE").getInt(null);
						gatt = (BluetoothGatt) m.invoke(device, activity, false, this, transport);
					} catch (Exception e) {
						e.printStackTrace();
						Log.d(TAG, " Catch to call normal connection");
						gatt = device.connectGatt(activity.getApplicationContext(), false, this);
					}
				}
			});
		} else {
			if (gatt != null) {
				callback.invoke();
			} else {
				callback.invoke("BluetoothGatt is null");
			}
		}
	}
	// bt_btif : Register with GATT stack failed.

	public void disconnect(final boolean force, final boolean isTimeout, final Callback callback) {
		connectCallback = null;
		clearBuffers();
		commandQueue.clear();
		commandQueueBusy = false;
		mainHandler.post(() -> {
			if (gatt != null) {
				try {
					if(connected){
						connected = false;
						gatt.disconnect();
					}
					if (force) {
						gatt.close();
					}
					if(isTimeout) {
						sendConnectionEvent(device, "BleManagerDisconnectPeripheral", GATT_CONNECT_TIMEOUT);
					}else {
						sendConnectionEvent(device, "BleManagerDisconnectPeripheral", BluetoothGatt.GATT_SUCCESS);
					}
					Log.d(BleManager.LOG_TAG, "Disconnect " +device.getAddress());
				} catch (Exception e) {
					sendConnectionEvent(device, "BleManagerDisconnectPeripheral", BluetoothGatt.GATT_FAILURE);
					Log.d(BleManager.LOG_TAG, "Error on disconnect", e);
				}
			} else{
				Log.d(BleManager.LOG_TAG, "GATT is null");
				sendConnectionEvent(device, "BleManagerDisconnectPeripheral", GATT_IS_NULL);
			}

			if (callback != null) {
				callback.invoke();
			}

		});
	}

	public WritableMap asWritableMap() {
		WritableMap map = Arguments.createMap();
		WritableMap advertising = Arguments.createMap();

		try {
			map.putString("name", device.getName());
			map.putString("id", device.getAddress()); // mac address
			map.putInt("rssi", advertisingRSSI);

			String name = device.getName();
			if (name != null)
				advertising.putString("localName", name);

			advertising.putMap("manufacturerData", byteArrayToWritableMap(advertisingDataBytes));

			// No scanResult to access so we can't check if peripheral is connectable
			advertising.putBoolean("isConnectable", true);

			map.putMap("advertising", advertising);
		} catch (Exception e) { // this shouldn't happen
			e.printStackTrace();
		}

		return map;
	}

	public WritableMap asWritableMap(BluetoothGatt gatt) {

		WritableMap map = asWritableMap();

		WritableArray servicesArray = Arguments.createArray();
		WritableArray characteristicsArray = Arguments.createArray();

		if (connected && gatt != null) {
			for (Iterator<BluetoothGattService> it = gatt.getServices().iterator(); it.hasNext();) {
				BluetoothGattService service = it.next();
				WritableMap serviceMap = Arguments.createMap();
				serviceMap.putString("uuid", UUIDHelper.uuidToString(service.getUuid()));

				for (Iterator<BluetoothGattCharacteristic> itCharacteristic = service.getCharacteristics()
						.iterator(); itCharacteristic.hasNext();) {
					BluetoothGattCharacteristic characteristic = itCharacteristic.next();
					WritableMap characteristicsMap = Arguments.createMap();

					characteristicsMap.putString("service", UUIDHelper.uuidToString(service.getUuid()));
					characteristicsMap.putString("characteristic", UUIDHelper.uuidToString(characteristic.getUuid()));

					characteristicsMap.putMap("properties", Helper.decodeProperties(characteristic));

					if (characteristic.getPermissions() > 0) {
						characteristicsMap.putMap("permissions", Helper.decodePermissions(characteristic));
					}

					WritableArray descriptorsArray = Arguments.createArray();

					for (BluetoothGattDescriptor descriptor : characteristic.getDescriptors()) {
						WritableMap descriptorMap = Arguments.createMap();
						descriptorMap.putString("uuid", UUIDHelper.uuidToString(descriptor.getUuid()));
						if (descriptor.getValue() != null) {
							descriptorMap.putString("value",
									Base64.encodeToString(descriptor.getValue(), Base64.NO_WRAP));
						} else {
							descriptorMap.putString("value", null);
						}

						if (descriptor.getPermissions() > 0) {
							descriptorMap.putMap("permissions", Helper.decodePermissions(descriptor));
						}
						descriptorsArray.pushMap(descriptorMap);
					}
					if (descriptorsArray.size() > 0) {
						characteristicsMap.putArray("descriptors", descriptorsArray);
					}
					characteristicsArray.pushMap(characteristicsMap);
				}
				servicesArray.pushMap(serviceMap);
			}
			map.putArray("services", servicesArray);
			map.putArray("characteristics", characteristicsArray);
		}

		return map;
	}

	static WritableMap byteArrayToWritableMap(byte[] bytes) throws JSONException {
		WritableMap object = Arguments.createMap();
		object.putString("CDVType", "ArrayBuffer");
		object.putString("data", bytes != null ? Base64.encodeToString(bytes, Base64.NO_WRAP) : null);
		object.putArray("bytes", bytes != null ? BleManager.bytesToWritableArray(bytes) : null);
		return object;
	}

	public boolean isConnected() {
		return connected;
	}

	public boolean isConnecting() {
		return connecting;
	}

	public BluetoothDevice getDevice() {
		return device;
	}

	@Override
	public void onServicesDiscovered(BluetoothGatt gatt, int status) {
		isConnectTimeout = false;
		super.onServicesDiscovered(gatt, status);
		Log.d(TAG, device.getAddress()+" onServicesDiscoveredStatus: "+ status);
		Log.d(TAG, device.getAddress()+" onServicesDiscoveredServiceSize: "+ gatt.getServices().size());
		mainHandler.post(() -> {
			if (retrieveServicesCallback != null) {
				WritableMap map = this.asWritableMap(gatt);
				retrieveServicesCallback.invoke(null, map);
				retrieveServicesCallback = null;
			}
			completedCommand();
		});
		//connect等待服务成功后回调
		sendConnectionEvent(device, "BleManagerConnectPeripheral", status);
		if (connectCallback != null) {
			Log.d(BleManager.LOG_TAG, "Connected to: " + device.getAddress());
			connectCallback.invoke();
			connectCallback = null;
		}
	}

	@Override
	public void onServiceChanged(BluetoothGatt gatt){
		Log.d(TAG, "onServiceChanged: "+gatt.toString());
	}

	@Override
	public void onConnectionStateChange(BluetoothGatt stateGatt, int status, final int newState) {

		Log.d(BleManager.LOG_TAG, "onConnectionStateChange to Connect newState=" + (newState == BluetoothProfile.STATE_CONNECTED) + " on peripheral: " + device.getAddress()
				+ " with BluetoothGatt status=" + (status == BluetoothGatt.GATT_SUCCESS));

		mainHandler.post(() -> {
			connecting = false;
			//连接操作回调
			if (newState == BluetoothProfile.STATE_CONNECTED && status == BluetoothGatt.GATT_SUCCESS) {
				connected = true;
				mainHandler.post(()->{
					retrieveServices(null);
				});
//				sendConnectionEvent(device, "BleManagerConnectPeripheral", status);

//				if (connectCallback != null) {
//					Log.d(BleManager.LOG_TAG, "Connected to: " + device.getAddress());
//					connectCallback.invoke();
//					connectCallback = null;
//				}

			} else if (newState == BluetoothProfile.STATE_DISCONNECTED || status != BluetoothGatt.GATT_SUCCESS) {
				if (discoverServicesRunnable != null) {
					mainHandler.removeCallbacks(discoverServicesRunnable);
					discoverServicesRunnable = null;
				}

				List<Callback> callbacks = Arrays.asList(writeCallback, retrieveServicesCallback, readRSSICallback,
						readCallback, registerNotifyCallback, requestMTUCallback);
				for (Callback currentCallback : callbacks) {
					if (currentCallback != null) {
						try {
							currentCallback.invoke("Device disconnected");
						} catch (Exception e) {
							e.printStackTrace();
						}				}
				}
				if (connectCallback != null) {
					connectCallback.invoke("Connection error, status="+status+",newState="+newState);
				}
				writeCallback = null;
				writeQueue.clear();
				readCallback = null;
				retrieveServicesCallback = null;
				readRSSICallback = null;
				registerNotifyCallback = null;
				requestMTUCallback = null;
				commandQueue.clear();
				commandQueueBusy = false;
				connectCallback = null;
				connected = false;
				clearBuffers();
				//已经是连接状态，无需disconnect，直接close释放
				stateGatt.close();
				sendConnectionEvent(device, "BleManagerDisconnectPeripheral", BluetoothGatt.GATT_SUCCESS);
			}

		});

	}

	public void updateRssi(int rssi) {
		advertisingRSSI = rssi;
	}

	public void updateData(byte[] data) {
		advertisingDataBytes = data;
	}

	public int unsignedToBytes(byte b) {
		return b & 0xFF;
	}

	//////

	@Override
	public void onCharacteristicChanged(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic) {
		super.onCharacteristicChanged(gatt, characteristic);
		try {
			String charString = characteristic.getUuid().toString();
			String service = characteristic.getService().getUuid().toString();
			NotifyBufferContainer buffer = this.bufferedCharacteristics
					.get(this.bufferedCharacteristicsKey(service, charString));
			byte[] dataValue = characteristic.getValue();
			if (buffer != null) {
				buffer.put(dataValue);
				Log.d(BleManager.LOG_TAG, "onCharacteristicChanged-buffering: " +
						buffer.size() + " from peripheral: " + device.getAddress());

				if (buffer.isBufferFull()) {
					Log.d(BleManager.LOG_TAG, "onCharacteristicChanged sending buffered data " + buffer.size());

					// send'm and reset
					dataValue = buffer.items.array();
					buffer.resetBuffer();
				} else {
					return;
				}
			}
			Log.d(BleManager.LOG_TAG, "onCharacteristicChanged: " + BleManager.bytesToHex(dataValue)
					+ " from peripheral: " + device.getAddress());
			WritableMap map = Arguments.createMap();
			map.putString("peripheral", device.getAddress());
			map.putString("characteristic", charString);
			map.putString("service", service);
			map.putArray("value", BleManager.bytesToWritableArray(dataValue));
			sendEvent("BleManagerDidUpdateValueForCharacteristic", map);

		} catch (Exception e) {
			Log.d(BleManager.LOG_TAG, "onCharacteristicChanged ERROR: " + e.toString());
		}
	}

	@Override
	public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
		super.onCharacteristicRead(gatt, characteristic, status);

		mainHandler.post(() -> {
			if (status != BluetoothGatt.GATT_SUCCESS) {
				if (status == GATT_AUTH_FAIL || status == GATT_INSUFFICIENT_AUTHENTICATION) {
					Log.d(BleManager.LOG_TAG, "Read needs bonding");
				}
				readCallback.invoke("Error reading " + characteristic.getUuid() + " status=" + status, null);
				readCallback = null;
			} else if (readCallback != null) {
				final byte[] dataValue = copyOf(characteristic.getValue());
				readCallback.invoke(null, BleManager.bytesToWritableArray(dataValue));
				readCallback = null;
			}
			completedCommand();
		});

	}

	@Override
	public void onCharacteristicWrite(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
		super.onCharacteristicWrite(gatt, characteristic, status);

		mainHandler.post(() -> {
			if (writeQueue.size() > 0) {
				byte[] data = writeQueue.get(0);
				writeQueue.remove(0);
				doWrite(characteristic, data, writeCallback);
			} else if (status != BluetoothGatt.GATT_SUCCESS) {
				if (status == GATT_AUTH_FAIL || status == GATT_INSUFFICIENT_AUTHENTICATION) {
					Log.d(BleManager.LOG_TAG, "Write needs bonding");
					// *not* doing completedCommand()
					return;
				}
				writeCallback.invoke( "Error writing " + characteristic.getUuid() + " status=" + status, null);
				writeCallback = null;
			} else if (writeCallback != null) {
				writeCallback.invoke();
				writeCallback = null;
			}
			completedCommand();
		});
	}

	@Override
	public void onDescriptorWrite(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
		mainHandler.post(() -> {
			if (registerNotifyCallback != null) {
				if (status == BluetoothGatt.GATT_SUCCESS) {
					isNotifyTimeout = false;
					registerNotifyCallback.invoke();
					Log.d(BleManager.LOG_TAG, "onDescriptorWrite success");
				} else {
					registerNotifyCallback.invoke("Error writing descriptor status=" + status, null);
					Log.e(BleManager.LOG_TAG, "Error writing descriptor status=" + status);
				}

				registerNotifyCallback = null;
			} else {
				Log.e(BleManager.LOG_TAG, "onDescriptorWrite with no callback");
			}

			completedCommand();
		});
	}

	@Override
	public void onReadRemoteRssi(BluetoothGatt gatt, int rssi, int status) {
		super.onReadRemoteRssi(gatt, rssi, status);

		mainHandler.post(() -> {
			if (readRSSICallback != null) {
				if (status == BluetoothGatt.GATT_SUCCESS) {
					updateRssi(rssi);
					readRSSICallback.invoke(null, rssi);
				} else {
					readRSSICallback.invoke("Error reading RSSI status=" + status, null);
				}

				readRSSICallback = null;
			}

			completedCommand();
		});
	}

	private String bufferedCharacteristicsKey(String serviceUUID, String characteristicUUID) {
		return serviceUUID + "-" + characteristicUUID;
	}

	private void clearBuffers() {
		for (Map.Entry<String, NotifyBufferContainer> entry : this.bufferedCharacteristics.entrySet())
			entry.getValue().resetBuffer();
	}

	private void setNotify(UUID serviceUUID, UUID characteristicUUID, final Boolean notify, Callback callback) {
		if (! isConnected() || gatt == null) {
			callback.invoke("Device is not connected", null);
			completedCommand();
			return;
		}

		BluetoothGattService service = getGattService(serviceUUID);
		if(service == null){
			callback.invoke("get BluetoothGattService is null.");
			completedCommand();
			Log.d(BleManager.LOG_TAG, device.getAddress()+" BluetoothGattService is null");
			return;
		}
		final BluetoothGattCharacteristic characteristic = findNotifyCharacteristic(service, characteristicUUID);

		if (characteristic == null) {
			callback.invoke("Characteristic " + characteristicUUID + " not found");
			completedCommand();
			Log.d(BleManager.LOG_TAG, device.getAddress()+" Characteristic is null");
			return;
		}

		final BluetoothGattDescriptor descriptor = characteristic.getDescriptor(UUIDHelper.uuidFromString(CHARACTERISTIC_NOTIFICATION_CONFIG));
		if (descriptor == null) {
			callback.invoke("Set notification failed for " + characteristicUUID);
			completedCommand();
			Log.d(BleManager.LOG_TAG, device.getAddress()+" descriptor is null");
			return;
		}

		// Prefer notify over indicate
		byte[] value;
		if ((characteristic.getProperties() & BluetoothGattCharacteristic.PROPERTY_NOTIFY) != 0) {
			Log.d(BleManager.LOG_TAG, "Characteristic " + characteristicUUID + " set NOTIFY");
			value = notify ? BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE : BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE;
		} else if ((characteristic.getProperties() & BluetoothGattCharacteristic.PROPERTY_INDICATE) != 0) {
			Log.d(BleManager.LOG_TAG, "Characteristic " + characteristicUUID + " set INDICATE");
			value = notify ? BluetoothGattDescriptor.ENABLE_INDICATION_VALUE : BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE;
		} else {
			String msg = "Characteristic " + characteristicUUID + " does not have NOTIFY or INDICATE property set";
			Log.d(BleManager.LOG_TAG, msg);
			callback.invoke(msg);
			completedCommand();
			return;
		}
		final byte[] finalValue = notify ? value : BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE;

		boolean isSetNotifySuccess = false;
		try {
			isSetNotifySuccess = gatt.setCharacteristicNotification(characteristic, notify);
			if (!isSetNotifySuccess) {
				callback.invoke("Failed to register notification for " + characteristicUUID);
				completedCommand();
				Log.d(BleManager.LOG_TAG, device.getAddress()+" setCharacteristicNotification is failed");
				return;
			}
			// Then write to descriptor
			descriptor.setValue(finalValue);
			registerNotifyCallback = callback;
			isSetNotifySuccess = gatt.writeDescriptor(descriptor);
		} catch(Exception e) {
			Log.d(BleManager.LOG_TAG, "Exception in setNotify", e);
		}
		Log.d(TAG, "setNotify isSetNotifySuccess: "+isSetNotifySuccess);
		if (!isSetNotifySuccess) {
			callback.invoke( "writeDescriptor failed for descriptor: " + descriptor.getUuid(), null);
			registerNotifyCallback = null;
			completedCommand();
			Log.d(BleManager.LOG_TAG, device.getAddress()+" writeDescriptor is failed");
		}
	}

	public void registerNotify(UUID serviceUUID, UUID characteristicUUID, Integer buffer, Callback callback) {
		if (!mainHandler.post(() -> {
			//正常情况下注册通知20ms完成
			mainHandler.postDelayed(()->{
				if(isNotifyTimeout){
					disconnect(true,true,null);
				}
			}, 600);
			isNotifyTimeout= true;
			Log.d(BleManager.LOG_TAG, device.getAddress()+" registerNotify");
			if (buffer > 1) {
				Log.d(BleManager.LOG_TAG, "registerNotify using buffer");
				String bufferKey = this.bufferedCharacteristicsKey(serviceUUID.toString(), characteristicUUID.toString());
				this.bufferedCharacteristics.put(bufferKey, new NotifyBufferContainer(buffer));
			}
			this.setNotify(serviceUUID, characteristicUUID, true, callback);
		})) {
			Log.e(BleManager.LOG_TAG, "Could not setNotify command to register notify");
		}
	}

	public void removeNotify(UUID serviceUUID, UUID characteristicUUID, Callback callback) {
		if (!mainHandler.post(() -> {
			Log.d(BleManager.LOG_TAG, "removeNotify");
			String bufferKey = this.bufferedCharacteristicsKey(serviceUUID.toString(), characteristicUUID.toString());
			if (this.bufferedCharacteristics.containsKey(bufferKey)) {
				NotifyBufferContainer buffer = this.bufferedCharacteristics.get(bufferKey);
				this.bufferedCharacteristics.remove(bufferKey);
			}
			this.setNotify(serviceUUID, characteristicUUID, false, callback);
		})) {
			Log.e(BleManager.LOG_TAG, "Could not setNotify command to remove notify");
		}
	}

	// Some devices reuse UUIDs across characteristics, so we can't use
	// service.getCharacteristic(characteristicUUID)
	// instead check the UUID and properties for each characteristic in the service
	// until we find the best match
	// This function prefers Notify over Indicate
	private BluetoothGattCharacteristic findNotifyCharacteristic(BluetoothGattService service,
																 UUID characteristicUUID) {

		try {
			// Check for Notify first
			List<BluetoothGattCharacteristic> characteristics = service.getCharacteristics();
			for (BluetoothGattCharacteristic characteristic : characteristics) {
				if ((characteristic.getProperties() & BluetoothGattCharacteristic.PROPERTY_NOTIFY) != 0
						&& characteristicUUID.equals(characteristic.getUuid())) {
					return characteristic;
				}
			}

			// If there wasn't Notify Characteristic, check for Indicate
			for (BluetoothGattCharacteristic characteristic : characteristics) {
				if ((characteristic.getProperties() & BluetoothGattCharacteristic.PROPERTY_INDICATE) != 0
						&& characteristicUUID.equals(characteristic.getUuid())) {
					return characteristic;
				}
			}

			// As a last resort, try and find ANY characteristic with this UUID, even if it
			// doesn't have the correct properties
			return service.getCharacteristic(characteristicUUID);
		} catch (Exception e) {
			Log.e(BleManager.LOG_TAG, "Error retriving characteristic " + characteristicUUID, e);
			return null;
		}
	}

	public void read(UUID serviceUUID, UUID characteristicUUID, final Callback callback) {
		mainHandler.post(() -> {
			if (!isConnected() || gatt == null) {
				callback.invoke("Device is not connected", null);
				completedCommand();
				return;
			}

			BluetoothGattService service = gatt.getService(serviceUUID);
			final BluetoothGattCharacteristic characteristic = findReadableCharacteristic(service, characteristicUUID);

			if (characteristic == null) {
				callback.invoke("Characteristic " + characteristicUUID + " not found.", null);
				completedCommand();
				return;
			}

			readCallback = callback;
			if (!gatt.readCharacteristic(characteristic)) {
				callback.invoke("Read failed", null);
				readCallback = null;
				completedCommand();
			}

		});
	}

	private byte[] copyOf(byte[] source) {
		if (source == null) return new byte[0];
		final int sourceLength = source.length;
		final byte[] copy = new byte[sourceLength];
		System.arraycopy(source, 0, copy, 0, sourceLength);
		return copy;
	}

	private boolean enqueue(Runnable command) {
		final boolean result = commandQueue.add(command);
		if (result) {
			nextCommand();
		} else {
			Log.d(BleManager.LOG_TAG, "could not enqueue command");
		}
		return result;
	}

	private void completedCommand() {
		commandQueue.poll();
		commandQueueBusy = false;
		nextCommand();
	}

	private void nextCommand() {
		synchronized (this) {
			if (commandQueueBusy) {
				Log.d(BleManager.LOG_TAG, "Command queue busy");
				return;
			}

			final Runnable nextCommand = commandQueue.peek();
			if (nextCommand == null) {
				Log.d(BleManager.LOG_TAG, "Command queue empty");
				return;
			}

			// Check if we still have a valid gatt object
			if (gatt == null) {
				Log.d(BleManager.LOG_TAG, "Error, gatt is null");
				commandQueue.clear();
				commandQueueBusy = false;
				return;
			}

			// Execute the next command in the queue
			commandQueueBusy = true;
			mainHandler.post(() -> {
				try {
					nextCommand.run();
				} catch (Exception ex) {
					Log.d(BleManager.LOG_TAG, "Error, command exception");
					completedCommand();
				}
			});
		}
	}

	public void readRSSI(final Callback callback) {
		mainHandler.post(()->{
			if (!isConnected()) {
				callback.invoke("Device is not connected", null);
				completedCommand();
				return;
			} else if (gatt == null) {
				callback.invoke("BluetoothGatt is null", null);
				completedCommand();
				return;
			} else {
				readRSSICallback = callback;
				if (!gatt.readRemoteRssi()) {
					callback.invoke("Read RSSI failed", null);
					readRSSICallback = null;
					completedCommand();
				}
			}
		});
	}

	public void refreshCache(Callback callback) {
		mainHandler.post(()->{
			try {
				Method localMethod = gatt.getClass().getMethod("refresh", new Class[0]);
				if (localMethod != null) {
					boolean res = ((Boolean) localMethod.invoke(gatt, new Object[0])).booleanValue();
					Log.e(TAG, "refreshCache " + res);
					if(callback != null) {
						callback.invoke(null, res);
					}
				} else {
					if(callback != null) {
						callback.invoke("Could not refresh cache for device.");
					}
				}
			} catch (Exception localException) {
				Log.e(TAG, "An exception occured while refreshing device");
				callback.invoke(localException.getMessage());
			} finally {
				completedCommand();
			}
		});
	}

//	private void retryDiscoverServices(Callback callback){
//		if(isConnected()) {
//			if (tryDiscoverCount == 3) {
//				Log.d(TAG, device.getAddress() + " - retryDiscoverServices disconnect");
//				disconnect(true, false, null);
//				return;
//			}
//			Log.d(TAG, device.getAddress() + " - retryDiscoverServices run");
//			//未发现服务成功
//			if (!isDiscover) {
//				tryDiscoverCount++;
//				this.retrieveServicesCallback = callback;
//				gatt.discoverServices();
//				Log.d(TAG, device.getAddress() + " - retryDiscoverServices try:" + tryDiscoverCount);
//				mainHandler.postDelayed(() -> retryDiscoverServices(callback), discoverTimeout);
//			}
//		}else {
//			Log.d(TAG, device.getAddress() + " - retryDiscoverServices is not connected");
//		}
//	}

	public void retrieveServices(Callback callback) {
		if (!isConnected()) {
			callback.invoke("Device is not connected", null);
		} else if (gatt == null) {
			callback.invoke("BluetoothGatt is null", null);
		} else {
			mainHandler.post(()->gatt.discoverServices());
		}
	}

	// Some peripherals re-use UUIDs for multiple characteristics so we need to
	// check the properties
	// and UUID of all characteristics instead of using
	// service.getCharacteristic(characteristicUUID)
	private BluetoothGattCharacteristic findReadableCharacteristic(BluetoothGattService service,
																   UUID characteristicUUID) {

		if (service != null) {
			int read = BluetoothGattCharacteristic.PROPERTY_READ;

			List<BluetoothGattCharacteristic> characteristics = service.getCharacteristics();
			for (BluetoothGattCharacteristic characteristic : characteristics) {
				if ((characteristic.getProperties() & read) != 0
						&& characteristicUUID.equals(characteristic.getUuid())) {
					return characteristic;
				}
			}

			// As a last resort, try and find ANY characteristic with this UUID, even if it
			// doesn't have the correct properties
			return service.getCharacteristic(characteristicUUID);
		}

		return null;
	}

	public boolean doWrite(final BluetoothGattCharacteristic characteristic, byte[] data, final Callback callback) {
		final byte[] copyOfData = copyOf(data);
		return mainHandler.post(() -> {
			characteristic.setValue(copyOfData);
			if (characteristic.getWriteType() == BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT)
				writeCallback = callback;
			else
				writeCallback = null;
			if (!gatt.writeCharacteristic(characteristic)) {
				// write without response, caller will handle the callback
				if (writeCallback != null) {
					writeCallback.invoke("Write failed", writeCallback);
					writeCallback = null;
				}
				completedCommand();
			}
		});
	}

	private BluetoothGattService getGattService(UUID serviceUUID){
		BluetoothGattService service;
		service = gatt.getService(serviceUUID);
		Log.d(TAG, device.getAddress()+" getGattService: "+ service);
		return service;
	}

	public void write(UUID serviceUUID, UUID characteristicUUID, byte[] data, Integer maxByteSize, Integer queueSleepTime, Callback callback, int writeType) {
		mainHandler.post(() -> {
			if (!isConnected() || gatt == null) {
				callback.invoke("Device is not connected", null);
				completedCommand();
				return;
			}

			BluetoothGattService service = getGattService(serviceUUID);
			if(service == null){
				callback.invoke("get BluetoothGattService is null.");
			}
			BluetoothGattCharacteristic characteristic = findWritableCharacteristic(service, characteristicUUID, writeType);

			if (characteristic == null) {
				callback.invoke("Characteristic " + characteristicUUID + " not found.");
				completedCommand();
				return;
			}

			characteristic.setWriteType(writeType);

			if (data.length <= maxByteSize) {
				if (! doWrite(characteristic, data, callback)) {
					callback.invoke("Write failed");
				} else {
					if (BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE == writeType) {
						callback.invoke();
					}
				}
			} else {
				int dataLength = data.length;
				int count = 0;
				byte[] firstMessage = null;
				List<byte[]> splitMessage = new ArrayList<>();

				while (count < dataLength && (dataLength - count > maxByteSize)) {
					if (count == 0) {
						firstMessage = Arrays.copyOfRange(data, count, count + maxByteSize);
					} else {
						byte[] message = Arrays.copyOfRange(data, count, count + maxByteSize);
						splitMessage.add(message);
					}
					count += maxByteSize;
				}
				if (count < dataLength) {
					// Other bytes in queue
					byte[] message = Arrays.copyOfRange(data, count, data.length);
					splitMessage.add(message);
				}

				if (BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT == writeType) {
					writeQueue.addAll(splitMessage);
					if (! doWrite(characteristic, firstMessage, callback)) {
						writeQueue.clear();
						callback.invoke("Write failed");
					}
				} else {
					try {
						boolean writeError = false;
						if (! doWrite(characteristic, firstMessage, callback)) {
							writeError = true;
							callback.invoke("Write failed");
						}
						if (! writeError) {
							Thread.sleep(queueSleepTime);
							for (byte[] message : splitMessage) {
								if (! doWrite(characteristic, message, callback)) {
									writeError = true;
									callback.invoke("Write failed");
									break;
								}
								Thread.sleep(queueSleepTime);
							}
							if (! writeError) {
								callback.invoke();
							}
						}
					} catch (InterruptedException e) {
						callback.invoke("Error during writing");
					}
				}
			}

			completedCommand();
		});
	}

	public void requestConnectionPriority(int connectionPriority, Callback callback) {
		mainHandler.post(() -> {
			if (gatt != null) {
				if (Build.VERSION.SDK_INT >= LOLLIPOP) {
					boolean status = gatt.requestConnectionPriority(connectionPriority);
					callback.invoke(null, status);
				} else {
					callback.invoke("Requesting connection priority requires at least API level 21", null);
				}
			} else {
				callback.invoke("BluetoothGatt is null", null);
			}

			completedCommand();
		});
	}

	public void requestMTU(int mtu, Callback callback) {
		mainHandler.post(() -> {
			if (!isConnected()) {
				callback.invoke("Device is not connected", null);
				completedCommand();
				return;
			}

			if (gatt == null) {
				callback.invoke("BluetoothGatt is null", null);
				completedCommand();
				return;
			}

			if (Build.VERSION.SDK_INT >= LOLLIPOP) {
				requestMTUCallback = callback;
				if (!gatt.requestMtu(mtu)) {
					requestMTUCallback.invoke("Request MTU failed", null);
					requestMTUCallback = null;
					completedCommand();
				}
			} else {
				callback.invoke("Requesting MTU requires at least API level 21", null);
				completedCommand();
			}
		});
	}

	@Override
	public void onMtuChanged(BluetoothGatt gatt, int mtu, int status) {
		super.onMtuChanged(gatt, mtu, status);
		mainHandler.post(() -> {
			if (requestMTUCallback != null) {
				if (status == BluetoothGatt.GATT_SUCCESS) {
					requestMTUCallback.invoke(null, mtu);
				} else {
					requestMTUCallback.invoke("Error requesting MTU status = " + status, null);
				}

				requestMTUCallback = null;
			}

			completedCommand();
		});
	}

	// Some peripherals re-use UUIDs for multiple characteristics so we need to
	// check the properties
	// and UUID of all characteristics instead of using
	// service.getCharacteristic(characteristicUUID)
	private BluetoothGattCharacteristic findWritableCharacteristic(BluetoothGattService service,
																   UUID characteristicUUID, int writeType) {
		try {
			// get write property
			int writeProperty = BluetoothGattCharacteristic.PROPERTY_WRITE;
			if (writeType == BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE) {
				writeProperty = BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE;
			}

			List<BluetoothGattCharacteristic> characteristics = service.getCharacteristics();
			for (BluetoothGattCharacteristic characteristic : characteristics) {
				if ((characteristic.getProperties() & writeProperty) != 0
						&& characteristicUUID.equals(characteristic.getUuid())) {
					return characteristic;
				}
			}

			// As a last resort, try and find ANY characteristic with this UUID, even if it
			// doesn't have the correct properties
			return service.getCharacteristic(characteristicUUID);
		} catch (Exception e) {
			Log.e(BleManager.LOG_TAG, "Error on findWritableCharacteristic", e);
			return null;
		}
	}

	private String generateHashKey(BluetoothGattCharacteristic characteristic) {
		return generateHashKey(characteristic.getService().getUuid(), characteristic);
	}

	private String generateHashKey(UUID serviceUUID, BluetoothGattCharacteristic characteristic) {
		return String.valueOf(serviceUUID) + "|" + characteristic.getUuid() + "|" + characteristic.getInstanceId();
	}

}
