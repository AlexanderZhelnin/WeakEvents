using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;

namespace WeakEvents;

public sealed class FastSmartWeakEvent<T> where T : class
{
    #region EventEntry
    struct EventEntry
    {
        public readonly FastSmartWeakEventForwarderProvider.ForwarderDelegate Forwarder;
        public readonly MethodInfo TargetMethod;
        public readonly WeakReference TargetReference;

        public EventEntry(FastSmartWeakEventForwarderProvider.ForwarderDelegate forwarder, MethodInfo targetMethod, WeakReference targetReference)
        {
            Forwarder = forwarder;
            TargetMethod = targetMethod;
            TargetReference = targetReference;
        }
    }
    #endregion
            
    private object _eventEntries = new List<EventEntry>();

    #region Конструкторы
    static FastSmartWeakEvent()
    {
        if (!typeof(T).IsSubclassOf(typeof(Delegate))) throw new ArgumentException("T must be a delegate type");
        var invoke = typeof(T).GetMethod("Invoke");
        if (invoke == null || invoke.GetParameters().Length != 2) throw new ArgumentException("T must be a delegate type taking 2 parameters");
        var senderParameter = invoke.GetParameters()[0];
        if (senderParameter.ParameterType != typeof(object)) throw new ArgumentException("The first delegate parameter must be of type 'object'");
        var argsParameter = invoke.GetParameters()[1];
        if (!(typeof(EventArgs).IsAssignableFrom(argsParameter.ParameterType))) throw new ArgumentException("The second delegate parameter must be derived from type 'EventArgs'");
        if (invoke.ReturnType != typeof(void)) throw new ArgumentException("The delegate return type must be void.");
    }

    public FastSmartWeakEvent() { }
    public FastSmartWeakEvent(T el) => Add(el);
    #endregion

    #region Add
    public void Add(T eh)
    {
        if (eh == null) throw new ArgumentException("Must be not null");
        lock (this)
            {
                var d = (Delegate)(object)eh;

                var targetMethod = d.Method;
                var targetInstance = d.Target;
                var target = targetInstance != null ? new WeakReference(targetInstance) : null;

                if (_eventEntries is List<EventEntry> ls)
                {
                    if (ls.Count == ls.Capacity) RemoveDeadEntries();
                    if (ls.Any(e => ReferenceEquals(e.TargetReference?.Target, targetInstance) && ReferenceEquals(e.TargetMethod, targetMethod))) return;

                    ls.Add(new EventEntry(FastSmartWeakEventForwarderProvider.GetForwarder(targetMethod), targetMethod, target));

                    if (ls.Count > 100) _eventEntries = List2Dictionary(ls);//ls.ToDictionary(e => $"{e.TargetReference.Target?.GetHashCode()}_{e.TargetMethod?.GetHashCode()}");

                }
                else if (_eventEntries is Dictionary<string, EventEntry> ds)
                {
                    var key = $"{targetInstance?.GetHashCode()}_{targetMethod?.GetHashCode()}";
                    if (ds.ContainsKey(key)) return;
                    ds.Add(key, new EventEntry(FastSmartWeakEventForwarderProvider.GetForwarder(targetMethod), targetMethod, target));
                }
            }
    }

    private static Dictionary<string, EventEntry> List2Dictionary(List<EventEntry> ls)
    {
        var d = new Dictionary<string, EventEntry>();
        foreach (var e in ls) d[$"{e.TargetReference?.Target?.GetHashCode()}_{e.TargetMethod?.GetHashCode()}"] = e;
        return d;

    }
    #endregion

    #region RemoveDeadEntries
    void RemoveDeadEntries()
    {
        if (_eventEntries is List<EventEntry> ls) ls.RemoveAll(ee => ee.TargetReference != null && !ee.TargetReference.IsAlive);
        else if (_eventEntries is Dictionary<string, EventEntry> ds) foreach (var k in ds.Keys.ToArray()) if (ds.TryGetValue(k, out var e) && e.TargetReference != null && !e.TargetReference.IsAlive) ds.Remove(k);
    }
    #endregion

    #region Remove
    public void Remove(T eh)
    {
        if (eh == null) throw new ArgumentException("Must be not null");
        lock (this)
            {
                var d = (Delegate)(object)eh;
                var targetInstance = d.Target;
                var targetMethod = d.Method;

                if (_eventEntries is List<EventEntry> ls)
                {
                    for (var i = ls.Count - 1; i >= 0; i--)
                    {
                        var entry = ls[i];
                        if (entry.TargetReference != null)
                        {
                            var target = entry.TargetReference.Target;
                            if (target == null) ls.RemoveAt(i);
                            else if (target == targetInstance && entry.TargetMethod == targetMethod) { ls.RemoveAt(i); break; }
                        }
                        else if (targetInstance == null && entry.TargetMethod == targetMethod) { ls.RemoveAt(i); break; }
                    }
                }
                else if (_eventEntries is Dictionary<string, EventEntry> ds) ds.Remove($"{targetInstance?.GetHashCode()}_{targetMethod?.GetHashCode()}");
            }
    }
    #endregion

    #region Raise
    public void Raise(object sender, EventArgs e)
    {
        var needsCleanup = false;
        EventEntry[] ent;
        lock (this)
        {
            if (_eventEntries is List<EventEntry> ls) ent = ls.ToArray();
            else if (_eventEntries is Dictionary<string, EventEntry> ds) ent = ds.Values.ToArray();
            else ent = Array.Empty<EventEntry>();
        }

        try { foreach (var ee in ent) try { needsCleanup |= ee.Forwarder(ee.TargetReference, sender, e); } catch { } } catch { }
        if (needsCleanup) lock (this) RemoveDeadEntries();
    }
    #endregion

    #region CanRaise
    public bool CanRaise
    {
        get
        {
            if (_eventEntries is List<EventEntry> ls) return ls.Count > 0;
            else if (_eventEntries is Dictionary<string, EventEntry> ds) return ds.Count > 0;
            return false;
        }
    }
    #endregion        
}

#region FastSmartWeakEventForwarderProvider
static class FastSmartWeakEventForwarderProvider
{
    static readonly MethodInfo _getTarget = typeof(WeakReference).GetMethod("get_Target");
    static readonly Type[] _forwarderParameters = { typeof(WeakReference), typeof(object), typeof(EventArgs) };
    internal delegate bool ForwarderDelegate(WeakReference wr, object sender, EventArgs e);

    static readonly Dictionary<MethodInfo, ForwarderDelegate> _forwarders = new Dictionary<MethodInfo, ForwarderDelegate>();

    internal static ForwarderDelegate GetForwarder(MethodInfo method)
    {
        lock (_forwarders) if (_forwarders.TryGetValue(method, out var d)) return d;

        if (method.DeclaringType.GetCustomAttributes(typeof(CompilerGeneratedAttribute), false).Length != 0) throw new ArgumentException("Cannot create weak event to anonymous method with closure.");
        var parameters = method.GetParameters();

        Debug.Assert(_getTarget != null);

        var dm = new DynamicMethod("FastSmartWeakEvent", typeof(bool), _forwarderParameters, method.DeclaringType);

        var il = dm.GetILGenerator();

        if (!method.IsStatic)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.EmitCall(OpCodes.Callvirt, _getTarget, null);
            il.Emit(OpCodes.Dup);
            var label = il.DefineLabel();
            il.Emit(OpCodes.Brtrue, label);
            il.Emit(OpCodes.Pop);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Ret);
            il.MarkLabel(label);
            // The castclass here is required for the generated code to be verifiable.
            // We can leave it out because we know this cast will always succeed
            // (the instance/method pair was taken from a delegate).
            // Unverifiable code is fine because private reflection is only allowed under FullTrust
            // anyways.
            //il.Emit(OpCodes.Castclass, method.DeclaringType);
        }
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldarg_2);
        // This castclass here is required to prevent creating a hole in the .NET type system.
        // See Program.TypeSafetyProblem in the 'SmartWeakEventBenchmark' to see the effect when
        // this cast is not used.
        // You can remove this cast if you trust add FastSmartWeakEvent.Raise callers to do
        // the right thing, but the small performance increase (about 5%) usually isn't worth the risk.
        il.Emit(OpCodes.Castclass, parameters[1].ParameterType);
        il.EmitCall(OpCodes.Call, method, null);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ret);

        var fd = (ForwarderDelegate)dm.CreateDelegate(typeof(ForwarderDelegate));
        lock (_forwarders) _forwarders[method] = fd;
        return fd;
    }
}
#endregion
