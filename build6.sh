#!/system/bin/sh
UB=/data/data/com.ai.assistance.operit/files/usr/var/lib/proot-distro/installed-rootfs/ubuntu
mount -t proc proc $UB/proc 2>/dev/null
mount --bind /dev/null $UB/dev/null 2>/dev/null
mount --bind /dev/urandom $UB/dev/urandom 2>/dev/null
mount --bind /dev/zero $UB/dev/zero 2>/dev/null
mount --bind /dev/random $UB/dev/random 2>/dev/null
echo "== copy fresh ImaGate project =="
rm -rf $UB/root/proj; mkdir -p $UB/root/proj
cp -r /sdcard/Download/Operit/android_projects/ImaGate/. $UB/root/proj/ 2>/dev/null; echo "copy rc=$?"
echo "== build =="
chroot $UB /bin/bash -c 'export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin; export HOME=/root; export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64; export ANDROID_HOME=/opt/android-sdk; export ANDROID_SDK_ROOT=/opt/android-sdk; cd /root/proj && sh gradlew assembleDebug --no-daemon --console=plain 2>&1 | tail -60'
echo "== apk =="
cp $UB/root/proj/app/build/outputs/apk/debug/app-debug.apk /data/local/tmp/imagate.apk 2>/dev/null && chmod 0644 /data/local/tmp/imagate.apk && ls -la /data/local/tmp/imagate.apk
echo DONE
