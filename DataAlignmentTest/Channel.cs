#region Code Labeling
//-----------------------------------------------------------------------------
//  Copyright (c) 2017 Schlumberger
//  Schlumberger Private
//-----------------------------------------------------------------------------
#endregion
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Storm.DataModel
{
    public class Channel
    {
        public string ChannelName { get; private set; }
        private readonly SortedList<long, double> _channelData = new SortedList<long, double>();

        public Channel(string channelName)
        {
            ChannelName = channelName;
        }

        public double this[long index] => _channelData[index];

        public void AddChannelValue(long index, double value)
        {
            if (_channelData.ContainsKey(index))
                _channelData[index] = value;
            else
                _channelData.Add(index, value);
        }

        public void RemoveChannelValue(long index)
        {
            if (_channelData.ContainsKey(index))
                _channelData.Remove(index);
        }

        public bool HasChannelIndex(long index)
        {
            return _channelData.ContainsKey(index);

        }

        public double GetChannelValue(long index)
        {
            if (_channelData.ContainsKey(index))
                return _channelData[index];
            return double.NaN;
        }

        public IList<long> GetIndex()
        {
            return _channelData.Keys.ToList();
        }

        public void DeleteChannelIndices(IEnumerable<long> indices)
        {
            foreach (var curIndex in indices)
            {
                RemoveChannelValue(curIndex);
            }
        }

        public long FindIndexWithTolanrance(long index, double tolanrance)
        {
            var lower = _channelData.LastOrDefault(pair => pair.Key <= index);
            var upper = _channelData.FirstOrDefault(pair => pair.Key >= index);

            if (lower.Equals(default(KeyValuePair<long, double>)) && upper.Equals(default(KeyValuePair<long, double>)))
            {
                return -1;
            }

            if (lower.Equals(default(KeyValuePair<long, double>)) && (upper.Key - index) < tolanrance)
            {
                return upper.Key;
            }

            if (!lower.Equals(default(KeyValuePair<long, double>)) && !upper.Equals(default(KeyValuePair<long, double>)))
            {
                long lowerDiff = index - lower.Key;
                long upperDiff = upper.Key - index;
                if (lowerDiff <= upperDiff)
                {
                    return lower.Key;
                }
                else
                {
                    return upper.Key;
                }
            }

            return -1;
        }
    }
}